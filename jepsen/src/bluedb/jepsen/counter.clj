(ns bluedb.jepsen.counter
  "Counter workload — targets the autocommit read-modify-write path.

  `:add` issues a single autocommit `UPDATE cnt SET n = n + delta WHERE id = 1`
  (a self-contained read-modify-write) via POST /sql; `:read` reads n via GET
  /tables/cnt (lock-free). `jepsen.checker/counter` flags any read below the sum
  of acknowledged increments — i.e. a lost update from two concurrent RMWs that
  both read the same n and both wrote n+1.

  This is the workload that probes the known gap: autocommit single-statement
  RMWs are only safe if they serialize, which is what the `--serialize-writes`
  /sql path (used here) provides."
  (:require [bluedb.jepsen.http :as h]
            [jepsen.client :as client]
            [clj-http.client :as http]
            [cheshire.core :as json]))

(defn- update! [node delta]
  (http/post (str (h/base node) "/sql")
             {:body (format "UPDATE cnt SET n = n + %d WHERE id = 1;" delta)
              :throw-exceptions false
              :socket-timeout 8000
              :connection-timeout 2000}))

(defn- read-counter [node]
  (let [r (http/get (str (h/base node) "/tables/cnt?id=eq.1")
                    {:throw-exceptions false :socket-timeout 5000 :connection-timeout 2000})]
    (when (= 200 (:status r))
      (some-> (json/parse-string (:body r) true) first :n))))

(defn- do-add [node delta]
  (try
    (case (:status (update! node delta))
      200 :ok
      503 :passive
      :failed)
    (catch java.net.ConnectException _ :down)
    (catch java.net.SocketTimeoutException _ :timeout)
    (catch Exception _ :timeout)))

(defrecord CounterClient [leader]
  client/Client
  (open! [this _test _node] this)
  (setup! [_this _test])

  (invoke! [_this _test op]
    (let [node (h/target leader)]
      (if (nil? node)
        (assoc op :type :fail :error :no-leader)
        (case (:f op)
          :add
          (case (do-add node (:value op))
            :ok      (assoc op :type :ok)
            :timeout (assoc op :type :info :error :timeout)
            ;; passive/down: didn't apply — retry once on the new leader.
            (let [node2 (h/refresh-leader! leader)]
              (if (and node2 (not= node2 node) (= :ok (do-add node2 (:value op))))
                (assoc op :type :ok)
                (assoc op :type :fail :error :no-writer))))

          :read
          (let [n (try (read-counter node) (catch Exception _ nil))]
            (if (some? n)
              (assoc op :type :ok :value n)
              (let [node2 (h/refresh-leader! leader)
                    n2    (when node2 (try (read-counter node2) (catch Exception _ nil)))]
                (if (some? n2)
                  (assoc op :type :ok :value n2)
                  (assoc op :type :fail :error :read-failed)))))))))

  (teardown! [_this _test])
  (close! [_this _test]))

(defn client []
  (->CounterClient (h/make-leader)))
