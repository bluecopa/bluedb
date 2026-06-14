(ns bluedb.jepsen.list-append
  "Elle list-append workload for bluedb.

  Each Jepsen transaction is a list of micro-ops — `[:append k v]` (append unique
  value v to the list at key k) and `[:r k nil]` (read the list at k) — executed
  as ONE SQL transaction:

    BEGIN; INSERT INTO la (k,v) VALUES (k,v); SELECT v FROM la WHERE k=k; ...; COMMIT;

  sent as a single POST /sql so it runs inside one explicit `BEGIN..COMMIT` on the
  active writer (which holds the write lease for the whole block). Elle's
  list-append checker reconstructs the transaction dependency graph from the
  observed reads and flags any serializability anomaly (G0/G1/G2, lost update,
  write skew, ...).

  Read order: `SELECT v FROM la WHERE k=?` (no ORDER BY) returns rows in
  row-key = insertion order, i.e. the order the writer actually appended them —
  which is exactly the list order Elle needs (NOT value-magnitude order, since
  commit order may differ from value order)."
  (:require [bluedb.jepsen.http :as h]
            [jepsen.client :as client]
            [clojure.string :as str]
            [clj-http.client :as http]
            [cheshire.core :as json]))

(defn- op->sql [[f k v]]
  (case f
    :append (format "INSERT INTO la (k, v) VALUES (%d, %d);" k v)
    :r      (format "SELECT v FROM la WHERE k = %d;" k)))

(defn- txn->sql [ops]
  (str "BEGIN; " (str/join " " (map op->sql ops)) " COMMIT;"))

(defn- post-sql
  "POST a raw SQL string to one node. Returns the ring response (status code),
  or throws on connection/timeout errors."
  [node sql]
  (http/post (str (h/base node) "/sql")
             {:body sql
              :throw-exceptions false
              :socket-timeout 8000
              :connection-timeout 2000}))

(defn- fill-reads
  "Map the micro-ops back over the per-statement payloads. `payloads` is the
  JSON array from /sql: [begin, op0, op1, ..., commit], so op i is at index i+1.
  An :append keeps its value; an :r is filled with the list it read."
  [ops payloads]
  (mapv (fn [op i]
          (let [[f k _] op
                payload (nth payloads (inc i) nil)]
            (if (= f :r)
              [:r k (mapv :v payload)] ; payload is a vector of {:v n}
              op)))
        ops
        (range)))

(defn- run-txn
  "Execute one transaction against `node`. Returns one of:
  {:ok value} | :passive | :down | :timeout | :failed."
  [node ops]
  (let [sql (txn->sql ops)]
    (try
      (let [r (post-sql node sql)]
        (cond
          (= 200 (:status r)) {:ok (fill-reads ops (json/parse-string (:body r) true))}
          (= 503 (:status r)) :passive
          :else :failed))
      (catch java.net.ConnectException _ :down)
      (catch java.net.SocketTimeoutException _ :timeout)
      (catch Exception _ :timeout))))

(defrecord ListAppendClient [leader]
  client/Client
  (open! [this _test _node] this)
  (setup! [_this _test])

  (invoke! [_this _test op]
    (let [node (h/target leader)]
      (if (nil? node)
        (assoc op :type :fail :error :no-leader)
        (let [res (run-txn node (:value op))]
          (cond
            (map? res)        (assoc op :type :ok :value (:ok res))
            (= res :timeout)  (assoc op :type :info :error :timeout)
            (= res :failed)   (assoc op :type :fail :error :sql-error)
            ;; :passive or :down — txn definitely didn't commit; retry once on
            ;; the freshly-discovered leader.
            :else
            (let [node2 (h/refresh-leader! leader)]
              (if (and node2 (not= node2 node))
                (let [res2 (run-txn node2 (:value op))]
                  (cond
                    (map? res2)       (assoc op :type :ok :value (:ok res2))
                    (= res2 :timeout) (assoc op :type :info :error :timeout)
                    :else             (assoc op :type :fail :error res2)))
                (assoc op :type :fail :error :no-writer))))))))

  (teardown! [_this _test])
  (close! [_this _test]))

(defn client []
  (->ListAppendClient (h/make-leader)))
