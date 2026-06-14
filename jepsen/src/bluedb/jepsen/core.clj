(ns bluedb.jepsen.core
  "Jepsen test entry point for bluedb.

  Two workloads (pick with --workload):

  * `set` (default) — a grow-only set. Clients append unique ints through the
    active writer; a final read reads the whole set back. `set-full` proves every
    acknowledged add survives (no lost writes) and nothing is fabricated. Best for
    durability / no-lost-update under failover.

  * `list-append` — Elle list-append. Each transaction is a `BEGIN..COMMIT` of
    appends + reads over several keys; Elle reconstructs the dependency graph and
    flags serializability anomalies (G0/G1/G2, write skew, lost update). Exercises
    the explicit-transaction path under concurrency.

  Faults (--nemesis): none | kill | partition | partition-half | skew | pause |
  arbiter | storage | disk-full | mix | chaos, injected via the docker CLI (skew
  = libfaketime; pause = docker pause; partition-half = isolate a 2-node
  minority; arbiter/storage = freeze Postgres/MinIO; disk-full = fill MinIO's
  bounded data dir).

  Run against the up docker-compose cluster, e.g.:

    lein run test --workload list-append --nemesis mix --time-limit 120 \\
      --concurrency 10 --node node1 --node node2 --node node3"
  (:require [bluedb.jepsen.client :as bc]
            [bluedb.jepsen.counter :as bcnt]
            [bluedb.jepsen.http :as h]
            [bluedb.jepsen.list-append :as la]
            [bluedb.jepsen.nemesis :as bn]
            [bluedb.jepsen.unique :as bu]
            [clojure.tools.logging :refer [info]]
            [jepsen [cli :as cli]
                    [checker :as checker]
                    [db :as db]
                    [generator :as gen]
                    [os :as os]
                    [tests :as tests]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.tests.cycle.append :as append]))

(defn bluedb-db
  "A no-op DB (the cluster is managed by docker-compose). setup!/teardown! only
  (re)create a clean schema on the active writer — drop everything, then run the
  workload's DDL."
  []
  (reify db/DB
    (setup! [_ _test node]
      (when (= node (h/active-node))
        (info "resetting schema on" node)
        (h/exec-sql! node "DROP TABLE IF EXISTS jset;")
        (h/exec-sql! node "DROP TABLE IF EXISTS la;")
        (h/exec-sql! node "DROP TABLE IF EXISTS cnt;")
        (h/exec-sql! node "DROP TABLE IF EXISTS u;")
        (h/exec-sql! node "CREATE TABLE jset (v INTEGER);")
        (h/exec-sql! node "CREATE TABLE la (k INTEGER, v INTEGER);")
        (h/exec-sql! node "CREATE TABLE cnt (id INTEGER PRIMARY KEY, n INTEGER);")
        (h/exec-sql! node "INSERT INTO cnt VALUES (1, 0);")
        (h/exec-sql! node "CREATE TABLE u (id INTEGER PRIMARY KEY);")))
    (teardown! [_ _test node]
      (when (= node (h/active-node))
        (h/exec-sql! node "DROP TABLE IF EXISTS jset;")
        (h/exec-sql! node "DROP TABLE IF EXISTS la;")
        (h/exec-sql! node "DROP TABLE IF EXISTS cnt;")
        (h/exec-sql! node "DROP TABLE IF EXISTS u;")))))

(defn- set-workload
  "Grow-only set: infinite stream of unique-int adds + a final whole-set read."
  [_opts]
  {:client          (bc/set-client)
   :generator       (map (fn [v] {:type :invoke :f :add :value v}) (range))
   :final-generator (gen/each-thread {:type :invoke :f :read})
   :checker         (checker/set-full {:linearizable? false})})

(defn- list-append-workload
  "Elle list-append over a handful of keys; checked against the requested
  consistency model (--consistency, default serializable)."
  [opts]
  (let [model (keyword (:consistency opts "serializable"))
        base (append/test {:key-count          8
                           :min-txn-length     1
                           :max-txn-length     4
                           :max-writes-per-key 16
                           :consistency-models [model]})]
    {:client          (la/client)
     :generator       (:generator base)
     :final-generator (:final-generator base)
     :checker         (:checker base)}))

(defn- counter-workload
  "Concurrent autocommit increments of one shared counter; checked that no
  acknowledged increment is lost (the autocommit read-modify-write path)."
  [_opts]
  {:client          (bcnt/client)
   :generator       (gen/mix [(map (constantly {:type :invoke :f :add :value 1}) (range))
                              (map (constantly {:type :invoke :f :read}) (range))])
   :final-generator (gen/each-thread {:type :invoke :f :read})
   :checker         (checker/counter)})

(defn- unique-workload
  "Concurrent INSERT/DELETE over a small id space; checked that the database
  never acknowledges two inserts of the same primary key (the same-PK race)."
  [_opts]
  {:client          (bu/client)
   ;; Inserts only (no reuse → sound checker). A TINY id space + no stagger +
   ;; high concurrency maximizes the chance two clients insert the same fresh PK
   ;; within the commit window (the narrow TOCTOU the fix closes).
   :stagger         0
   :generator       (repeatedly (fn [] {:type :invoke :f :insert :value (rand-int 4)}))
   :final-generator (gen/once {:type :invoke :f :insert :value 999999})
   :checker         (bu/checker)})

(def fault-cycles
  "Maps --nemesis to the cycle of nemesis ops. Sleeps straddle the lease TTL
  (10s) so failover completes inside a fault window."
  {"kill"      [(gen/sleep 6)  {:type :info :f :kill-writer}
                (gen/sleep 14) {:type :info :f :start-all}]
   "partition" [(gen/sleep 6)  {:type :info :f :partition-writer}
                (gen/sleep 14) {:type :info :f :heal}]
   "mix"       [(gen/sleep 6)  {:type :info :f :kill-writer}
                (gen/sleep 14) {:type :info :f :start-all}
                (gen/sleep 6)  {:type :info :f :partition-writer}
                (gen/sleep 14) {:type :info :f :heal}]
   "skew"      [(gen/sleep 6)  {:type :info :f :skew-clock}
                (gen/sleep 14) {:type :info :f :reset-clock}]
   "pause"     [(gen/sleep 6)  {:type :info :f :pause-writer}
                (gen/sleep 14) {:type :info :f :resume}]
   "partition-half" [(gen/sleep 6)  {:type :info :f :isolate-half}
                     (gen/sleep 14) {:type :info :f :heal}]
   "arbiter"   [(gen/sleep 6)  {:type :info :f :pause-postgres}
                (gen/sleep 16) {:type :info :f :resume-postgres}]
   "storage"   [(gen/sleep 6)  {:type :info :f :pause-minio}
                (gen/sleep 16) {:type :info :f :resume-minio}]
   "disk-full" [(gen/sleep 6)  {:type :info :f :fill-disk}
                (gen/sleep 16) {:type :info :f :free-disk}]
   "chaos"     [(gen/sleep 6)  {:type :info :f :kill-writer}
                (gen/sleep 14) {:type :info :f :start-all}
                (gen/sleep 5)  {:type :info :f :pause-writer}
                (gen/sleep 14) {:type :info :f :resume}
                (gen/sleep 5)  {:type :info :f :skew-clock}
                (gen/sleep 14) {:type :info :f :reset-clock}
                (gen/sleep 5)  {:type :info :f :isolate-half}
                (gen/sleep 14) {:type :info :f :heal}]
   "none"      []})

(defn bluedb-test
  [opts]
  (let [kind      (:nemesis opts "mix")
        cycle-ops (get fault-cycles kind (get fault-cycles "mix"))
        wname     (:workload opts "set")
        wl        ((case wname
                     "list-append" list-append-workload
                     "counter"     counter-workload
                     "unique"      unique-workload
                     set-workload)
                   opts)]
    (merge tests/noop-test
           opts
           {:name      (str "bluedb-" wname "-" kind)
            :os        os/noop
            :db        (bluedb-db)
            :client    (:client wl)
            :nemesis   (bn/nemesis)
            :ssh       {:dummy? true}
            :nodes     (vec (keys h/ports))
            :generator
            (gen/phases
             (->> (let [g (:generator wl), s (:stagger wl 1/50)]
                    (if (and s (pos? s)) (gen/stagger s g) g))
                  (gen/nemesis (when (seq cycle-ops) (gen/cycle cycle-ops)))
                  (gen/time-limit (:time-limit opts 120)))
             ;; recover everything (network, processes, infra, disk) before the
             ;; final read, so a run cut mid-outage still has a writer to read.
             (gen/nemesis (gen/once {:type :info :f :heal}))
             (gen/nemesis (gen/once {:type :info :f :start-all}))
             (gen/nemesis (gen/once {:type :info :f :resume}))
             (gen/nemesis (gen/once {:type :info :f :resume-postgres}))
             (gen/nemesis (gen/once {:type :info :f :resume-minio}))
             (gen/nemesis (gen/once {:type :info :f :free-disk}))
             (gen/sleep 25)
             (gen/clients (:final-generator wl)))
            :checker
            (checker/compose
             {:workload   (:checker wl)
              :timeline   (timeline/html)
              :stats      (checker/stats)
              :exceptions (checker/unhandled-exceptions)})})))

(def cli-opts
  "Extra command-line options beyond Jepsen's defaults."
  [[nil "--nemesis NAME"
    "Faults: kill|partition|partition-half|skew|pause|arbiter|storage|disk-full|mix|chaos|none"
    :default "mix"
    :validate [#{"kill" "partition" "partition-half" "skew" "pause"
                 "arbiter" "storage" "disk-full" "mix" "chaos" "none"}
               "unknown nemesis"]]
   [nil "--workload NAME" "Workload: set | list-append | counter | unique"
    :default "set"
    :validate [#{"set" "list-append" "counter" "unique"}
               "must be set, list-append, counter, or unique"]]
   [nil "--consistency MODEL" "list-append model: serializable | strict-serializable"
    :default "serializable"
    :validate [#{"serializable" "strict-serializable"}
               "must be serializable or strict-serializable"]]])

(defn -main [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn bluedb-test :opt-spec cli-opts})
                   (cli/serve-cmd))
            args))
