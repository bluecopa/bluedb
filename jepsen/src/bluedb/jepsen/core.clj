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
  mix | chaos, injected via the docker CLI (skew = wall-clock skew via
  libfaketime; pause = docker pause; partition-half = isolate a 2-node minority).

  Run against the up docker-compose cluster, e.g.:

    lein run test --workload list-append --nemesis mix --time-limit 120 \\
      --concurrency 10 --node node1 --node node2 --node node3"
  (:require [bluedb.jepsen.client :as bc]
            [bluedb.jepsen.counter :as bcnt]
            [bluedb.jepsen.http :as h]
            [bluedb.jepsen.list-append :as la]
            [bluedb.jepsen.nemesis :as bn]
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
        (h/exec-sql! node "CREATE TABLE jset (v INTEGER);")
        (h/exec-sql! node "CREATE TABLE la (k INTEGER, v INTEGER);")
        (h/exec-sql! node "CREATE TABLE cnt (id INTEGER PRIMARY KEY, n INTEGER);")
        (h/exec-sql! node "INSERT INTO cnt VALUES (1, 0);")))
    (teardown! [_ _test node]
      (when (= node (h/active-node))
        (h/exec-sql! node "DROP TABLE IF EXISTS jset;")
        (h/exec-sql! node "DROP TABLE IF EXISTS la;")
        (h/exec-sql! node "DROP TABLE IF EXISTS cnt;")))))

(defn- set-workload
  "Grow-only set: infinite stream of unique-int adds + a final whole-set read."
  [_opts]
  {:client          (bc/set-client)
   :generator       (map (fn [v] {:type :invoke :f :add :value v}) (range))
   :final-generator (gen/each-thread {:type :invoke :f :read})
   :checker         (checker/set-full {:linearizable? false})})

(defn- list-append-workload
  "Elle list-append over a handful of keys; checked for serializability."
  [_opts]
  (let [base (append/test {:key-count          8
                           :min-txn-length     1
                           :max-txn-length     4
                           :max-writes-per-key 16
                           :consistency-models [:serializable]})]
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
             (->> (:generator wl)
                  (gen/stagger 1/50)
                  (gen/nemesis (when (seq cycle-ops) (gen/cycle cycle-ops)))
                  (gen/time-limit (:time-limit opts 120)))
             (gen/nemesis (gen/once {:type :info :f :heal}))
             (gen/nemesis (gen/once {:type :info :f :start-all}))
             (gen/sleep 20)
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
    "Faults: kill | partition | partition-half | skew | pause | mix | chaos | none"
    :default "mix"
    :validate [#{"kill" "partition" "partition-half" "skew" "pause" "mix" "chaos" "none"}
               "must be kill, partition, partition-half, skew, pause, mix, chaos, or none"]]
   [nil "--workload NAME" "Workload: set | list-append | counter"
    :default "set"
    :validate [#{"set" "list-append" "counter"} "must be set, list-append, or counter"]]])

(defn -main [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn bluedb-test :opt-spec cli-opts})
                   (cli/serve-cmd))
            args))
