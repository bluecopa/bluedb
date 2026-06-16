(ns bluedb.jepsen.unique
  "Unique-insert workload — targets the concurrent same-primary-key INSERT race.

  Clients INSERT rows over a small id space (`POST /tables/u`), so many race to
  insert the SAME fresh primary key at once. Under correct uniqueness exactly one
  of them gets HTTP 200 and the rest get a duplicate-key error; if two inserts of
  the same id both get 200, the database acknowledged two rows with one PK — a
  violation. (Final-state inspection can't catch it — last-write-wins leaves one
  row — so we count acknowledgements from the history instead.)

  Inserts only, no deletes: an id is never reused, so the checker is sound
  (≤ 1 acked insert per id). Contention is the startup burst; run with
  `--test-count N` for more independent bursts."
  (:require [bluedb.jepsen.http :as h]
            [jepsen.client :as client]
            [jepsen.checker :as checker]
            [clj-http.client :as http]
            [cheshire.core :as json]))

;; Reset `u` once per run (the first client to win the CAS bootstraps it). The
;; schema regime needs an explicit PK'd table and no longer auto-creates one.
(defonce ^:private table-ready (atom false))

(defn- insert! [node id]
  (http/post (str (h/base node) "/tables/u")
             {:body (json/generate-string {:id id})
              :content-type :json
              :throw-exceptions false
              :socket-timeout 8000
              :connection-timeout 2000}))

(defn- delete! [node id]
  (http/delete (str (h/base node) (format "/tables/u?id=eq.%d" id))
               {:throw-exceptions false :socket-timeout 8000 :connection-timeout 2000}))

(defn- classify [r]
  ;; 200 = applied; anything else (incl. uniqueness violation 4xx/5xx) = did not.
  (cond (= 200 (:status r)) :ok
        (= 503 (:status r)) :passive
        :else               :rejected))

(defn- attempt [f node id]
  (try
    (classify ((case f :insert insert! :delete delete!) node id))
    (catch java.net.ConnectException _ :down)
    (catch java.net.SocketTimeoutException _ :timeout)
    (catch Exception _ :timeout)))

(defrecord UniqueClient [leader]
  client/Client
  (open! [this _test _node] this)

  (setup! [_this _test]
    (when (compare-and-set! table-ready false true)
      (h/reset-unique-table!)))

  (invoke! [_this _test op]
    (let [node (h/target leader)
          id   (:value op)]
      (if (nil? node)
        (assoc op :type :fail :error :no-leader)
        (case (attempt (:f op) node id)
          :ok       (assoc op :type :ok)
          :rejected (assoc op :type :fail :error :rejected) ; e.g. duplicate key
          :timeout  (assoc op :type :info :error :timeout)
          ;; passive/down: retry once on the freshly-discovered leader.
          (let [node2 (h/refresh-leader! leader)]
            (if (and node2 (not= node2 node))
              (case (attempt (:f op) node2 id)
                :ok       (assoc op :type :ok)
                :timeout  (assoc op :type :info :error :timeout)
                :rejected (assoc op :type :fail :error :rejected)
                (assoc op :type :fail :error :no-writer))
              (assoc op :type :fail :error :no-writer)))))))

  (teardown! [_this _test])
  (close! [_this _test]))

(defn client []
  (->UniqueClient (h/make-leader)))

(defn checker
  "Sound under an inserts-only history (ids never reused): every primary key may
  be acknowledged at most once. Flags any id with two or more :ok inserts."
  []
  (reify checker/Checker
    (check [_ _ history _]
      (let [ok-inserts (->> history
                            (filter #(and (= :ok (:type %)) (= :insert (:f %))))
                            (map :value))
            counts      (frequencies ok-inserts)
            dups        (into (sorted-map) (filter (fn [[_ c]] (> c 1)) counts))]
        {:valid?              (empty? dups)
         :duplicate-acked-ids dups
         :distinct-acked      (count counts)
         :ok-insert-count     (count ok-inserts)}))))
