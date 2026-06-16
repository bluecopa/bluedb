(ns bluedb.jepsen.dur
  "Durability probe — a grow-only set written through the **/sql autocommit**
  path (the path the counter workload exercised when it lost acked writes under
  `kill`), but with **identifiable** elements so a lost write can be named and
  timed.

  `:add v` → `INSERT INTO jset (v) VALUES (v)` via POST /sql; HTTP 200 = the write
  returned from `Db::write(await_durable=true)`, i.e. SlateDB reported the WAL SST
  durable to object storage before we acked. `:read` → the whole set back.

  The `set-full` checker reports the exact set of acked-but-missing elements
  (lost writes). On every ack we also emit a line

      DUR-ACK v=<v> node=<node> t=<epoch-ms>

  to stderr, so for each lost element we can recover *when* it was acked and on
  *which* node, and correlate that against the kill timeline (jepsen nemesis log)
  and the role/epoch timeline (a /admin/status poller). That tells us whether the
  lost writes were acked in a tight window right before the kill (an
  ack-vs-durable boundary) or spread across time (a deeper recovery loss)."
  (:require [bluedb.jepsen.http :as h]
            [jepsen.client :as client]))

;; Bootstrap jset once per run (same PK'd table the set workload uses).
(defonce ^:private table-ready (atom false))

(defn- do-add
  "Insert `v` via /sql on `node`. Returns :ok :passive :down :timeout, or nil."
  [node v]
  (try
    (let [code (:status (h/add-via-sql! node v))]
      (cond (= 200 code) :ok
            (= 503 code) :passive
            :else        nil))
    (catch java.net.ConnectException _ :down)
    (catch java.net.SocketTimeoutException _ :timeout)
    (catch Exception _ :timeout)))

(defn- log-ack! [v node]
  ;; Emitted on every acked add; grepped per lost-id during analysis.
  (binding [*out* *err*]
    (println (str "DUR-ACK v=" v " node=" node " t=" (System/currentTimeMillis)))))

(defrecord DurClient [leader]
  client/Client
  (open! [this _test _node] this)

  (setup! [_this _test]
    (when (compare-and-set! table-ready false true)
      (h/reset-set-table!)))

  (invoke! [_this _test op]
    (case (:f op)
      :add
      (let [node (h/target leader)]
        (if (nil? node)
          (assoc op :type :fail :error :no-leader)
          (case (do-add node (:value op))
            :ok      (do (log-ack! (:value op) node) (assoc op :type :ok))
            :timeout (assoc op :type :info :error :timeout)
            ;; passive/down: definitely didn't apply — retry once on the new leader.
            (let [node2 (h/refresh-leader! leader)]
              (if (and node2 (not= node2 node) (= :ok (do-add node2 (:value op))))
                (do (log-ack! (:value op) node2) (assoc op :type :ok))
                (assoc op :type :fail :error :no-writer))))))

      :read
      (let [node (h/target leader)
            s    (when node (try (h/read-set node) (catch Exception _ nil)))]
        (if s
          (assoc op :type :ok :value s)
          (let [node2 (h/refresh-leader! leader)
                s2    (when node2 (try (h/read-set node2) (catch Exception _ nil)))]
            (if s2
              (assoc op :type :ok :value s2)
              (assoc op :type :fail :error :read-failed)))))))

  (teardown! [_this _test])
  (close! [_this _test]))

(defn client []
  (->DurClient (h/make-leader)))
