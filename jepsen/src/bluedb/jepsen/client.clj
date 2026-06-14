(ns bluedb.jepsen.client
  "Leader-aware Jepsen client for the grow-only-set workload.

  :add  -> INSERT a unique int via the active writer.
  :read -> SELECT the whole set (final-read phase), via the active writer.

  Op outcome semantics matter for the checker:
    :ok   the write definitely happened (HTTP 200)
    :fail the write definitely did NOT happen (503 passive, or connection
          refused = request never reached a live server)
    :info indeterminate (timeout mid-flight) — checker must assume it *might*
          have applied."
  (:require [bluedb.jepsen.http :as h]
            [jepsen.client :as client]))

(defn- refresh-leader!
  "Discover the current active writer and cache it in the shared atom."
  [leader]
  (let [a (h/active-node)]
    (reset! leader a)
    a))

(defn- target [leader]
  (or @leader (refresh-leader! leader)))

(defn- do-add
  "Attempt a single INSERT against `node`. Returns one of :ok :passive :down
  :timeout, or nil for an unexpected status."
  [node v]
  (try
    (let [code (:status (h/add! node v))]
      (cond (= 200 code) :ok
            (= 503 code) :passive
            :else        nil))
    (catch java.net.ConnectException _ :down)
    (catch java.net.SocketTimeoutException _ :timeout)
    (catch Exception _ :timeout)))

(defrecord SetClient [leader]
  client/Client
  (open! [this _test _node] this)

  (setup! [_this _test])

  (invoke! [_this _test op]
    (case (:f op)
      :add
      (let [node (target leader)]
        (if (nil? node)
          (assoc op :type :fail :error :no-leader)
          (case (do-add node (:value op))
            :ok      (assoc op :type :ok)
            :timeout (assoc op :type :info :error :timeout)
            ;; passive or node down: re-discover and retry once on the new leader
            (let [node2 (refresh-leader! leader)]
              (if (and node2 (not= node2 node))
                (case (do-add node2 (:value op))
                  :ok      (assoc op :type :ok)
                  :timeout (assoc op :type :info :error :timeout)
                  (assoc op :type :fail :error :no-writer))
                (assoc op :type :fail :error :no-writer))))))

      :read
      ;; Final-phase authoritative read from the current writer.
      (let [node (target leader)
            s    (when node (try (h/read-set node) (catch Exception _ nil)))]
        (if s
          (assoc op :type :ok :value s)
          (let [node2 (refresh-leader! leader)
                s2    (when node2 (try (h/read-set node2) (catch Exception _ nil)))]
            (if s2
              (assoc op :type :ok :value s2)
              (assoc op :type :fail :error :read-failed)))))))

  (teardown! [_this _test])

  (close! [_this _test]))

(defn set-client []
  (->SetClient (atom nil)))
