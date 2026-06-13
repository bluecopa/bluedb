(ns bluedb.jepsen.core
  "Jepsen test entry point for bluedb.

  Workload: a grow-only set. Clients append unique integers through the active
  writer; a final-read phase reads the whole set back. The `set-full` checker
  proves that every *acknowledged* add survives (no lost writes) and that no
  element appears that was never added (no fabrication) — across writer kills
  and network partitions that force failover.

  Run against the already-up docker-compose cluster:

    lein run test --time-limit 120 --concurrency 10 --nemesis kill
    lein run test --time-limit 120 --concurrency 10 --nemesis partition
    lein run test --time-limit 180 --concurrency 10 --nemesis mix"
  (:require [bluedb.jepsen.client :as bc]
            [bluedb.jepsen.http :as h]
            [bluedb.jepsen.nemesis :as bn]
            [clojure.tools.logging :refer [info]]
            [jepsen [cli :as cli]
                    [checker :as checker]
                    [db :as db]
                    [generator :as gen]
                    [os :as os]
                    [tests :as tests]]
            [jepsen.checker.timeline :as timeline]))

(defn bluedb-db
  "A no-op DB: the cluster is managed by docker-compose, not Jepsen. We only use
  setup!/teardown! to (re)create a clean `jset` table on the active writer."
  []
  (reify db/DB
    (setup! [_ _test node]
      ;; Only the active writer can run DDL; the matching node does it once.
      (when (= node (h/active-node))
        (info "creating clean jset table on" node)
        (h/exec-sql! node "DROP TABLE IF EXISTS jset;")
        (h/exec-sql! node "CREATE TABLE jset (v INTEGER);")))
    (teardown! [_ _test node]
      (when (= node (h/active-node))
        (h/exec-sql! node "DROP TABLE IF EXISTS jset;")))))

(def fault-cycles
  "Maps the --nemesis option to the sequence of nemesis ops to cycle through.
  Sleeps straddle the lease TTL (10s) so failover fully completes inside a
  fault window."
  {"kill"      [(gen/sleep 6)  {:type :info :f :kill-writer}
                (gen/sleep 14) {:type :info :f :start-all}]
   "partition" [(gen/sleep 6)  {:type :info :f :partition-writer}
                (gen/sleep 14) {:type :info :f :heal}]
   "mix"       [(gen/sleep 6)  {:type :info :f :kill-writer}
                (gen/sleep 14) {:type :info :f :start-all}
                (gen/sleep 6)  {:type :info :f :partition-writer}
                (gen/sleep 14) {:type :info :f :heal}]
   "none"      []})

(defn bluedb-test
  [opts]
  (let [kind (:nemesis opts "mix")
        cycle-ops (get fault-cycles kind (get fault-cycles "mix"))]
    (merge tests/noop-test
           opts
           {:name      (str "bluedb-set-" kind)
            :os        os/noop
            :db        (bluedb-db)
            :client    (bc/set-client)
            :nemesis   (bn/nemesis)
            :ssh       {:dummy? true}
            :nodes     (vec (keys h/ports))
            :generator
            (gen/phases
             ;; main phase: append unique ints while faults churn the writer
             (->> (range)
                  (map (fn [v] {:type :invoke :f :add :value v}))
                  (gen/stagger 1/50)
                  (gen/nemesis (when (seq cycle-ops) (gen/cycle cycle-ops)))
                  (gen/time-limit (:time-limit opts 120)))
             ;; recover everything and let the cluster settle on one writer
             (gen/nemesis (gen/once {:type :info :f :heal}))
             (gen/nemesis (gen/once {:type :info :f :start-all}))
             (gen/sleep 20)
             ;; authoritative final read from the (re)settled writer
             (gen/clients (gen/each-thread {:type :invoke :f :read})))
            :checker
            (checker/compose
             {:set-full   (checker/set-full {:linearizable? false})
              :timeline   (timeline/html)
              :stats      (checker/stats)
              :exceptions (checker/unhandled-exceptions)})})))

(def cli-opts
  "Extra command-line options beyond Jepsen's defaults."
  [[nil "--nemesis NAME" "Fault schedule: kill | partition | mix | none"
    :default "mix"
    :validate [#{"kill" "partition" "mix" "none"} "must be kill, partition, mix, or none"]]])

(defn -main [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn bluedb-test :opt-spec cli-opts})
                   (cli/serve-cmd))
            args))
