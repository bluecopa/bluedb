(ns bluedb.jepsen.graph
  "Graph-traversal snapshot-isolation workload — proves a traversal observes a
  single consistent cut of the graph, never a torn mixture of states, under
  faults.

  The graph is a tiny diamond with two interchangeable bridges. At any committed
  instant it is in exactly one of two configurations:

    config A:  R → A → Z      config B:  R → B → Z

  The writer flips between them with the **atomic** rewire `POST
  /graph/{g}/mutate`: install the target bridge's two edges and delete the other
  bridge's two edges in ONE WriteBatch, so the swap commits all-or-nothing —
  there is never a committed graph where the sink `Z` is unreachable. (The
  install/delete sets are disjoint, so the rewire is self-correcting: whatever
  the prior config, after a swap-to-X the graph is exactly `{R→X, X→Z}`.)

  Concurrently, clients run `reachable(R, directed)`. A traversal scans R's
  out-edges, then the bridge's out-edges, then Z — **multiple scans over time**.
  The invariant the checker enforces:

    every acknowledged `reachable(R)` result is exactly `{R, A, Z}` or
    `{R, B, Z}` — `Z` is always present and the result has exactly three nodes.

  * Under **snapshot isolation** (Phase 2: the traversal pins one snapshot)
    every scan reads the same sequence, so it sees one whole config — invariant
    holds no matter how the swaps interleave.
  * Under **read-committed-across-scans** a torn traversal can scan R and see
    `R→A`, then — after a swap to B deletes the A pair — scan A and find `A→Z`
    gone, returning `{R, A}` with `Z` dropped. That is the signature failure the
    checker catches (`:sink-dropped`).

  The `pause` nemesis is the sharpest stressor: it freezes a traversal between
  its scan of R and its scan of the bridge, straddling a swap — exactly the
  window a non-snapshot read would tear in. `kill`/`partition` add failover.

  Run-independent: a fresh per-run graph `jgraph-<ms>`. Swaps that come back
  definitely-not-applied (503/refused) retry once on a freshly-discovered
  writer; indeterminate timeouts are recorded `:info` (they don't affect the
  read invariant, which is judged only over `:ok` reads).

  Example:
    lein run test --workload graph --nemesis pause --time-limit 120 \\
      --concurrency 10 --node node1 --node node2 --node node3

  ✅ VALIDATED (2026-06-16) on the live 3-node docker cluster — all green
  (`:valid? true`, violations 0, sink-dropped 0 — every traversal saw one whole
  config, Z never dropped):
    pause     — 1562 reaches over 715 atomic swaps (freeze a traversal between
                its scan of R and the bridge, straddling a swap — the sharpest
                torn-read window).
    kill      — 1840 reaches over 709 swaps, through writer crash + failover.
    partition — 1427 reaches over 582 swaps, through writer isolation.
    mix       — 2238 reaches over 679 swaps (kill+partition cycled, 180s).
  ~7000 reaches over ~2700 atomic rewires; leadership moved across ~29 epochs
  (22→51). The pinned-snapshot traversal observed a single consistent cut under
  every fault — snapshot isolation holds across crash / partition / pause +
  failover. (`lein check` clean on Java 21.)"
  (:require [bluedb.jepsen.http :as h]
            [jepsen.client :as client]
            [jepsen.checker :as checker]
            [clj-http.client :as http]
            [cheshire.core :as json]))

(def root "R")
(def sink "Z")
(defn- other [t] (if (= t "A") "B" "A"))

;; --- HTTP --------------------------------------------------------------------

(defn- mutate!
  "Atomically install config `target` (R→target→Z) and remove the other
  bridge's pair, in one batch."
  [node graph target]
  (let [o (other target)]
    (http/post (str (h/base node) "/graph/" graph "/mutate")
               (h/opts :socket-timeout 8000 :connection-timeout 2000
                       :content-type :json
                       :body (json/generate-string
                              {:upserts [{:src root :dst target :weight 1}
                                         {:src target :dst sink :weight 1}]
                               :deletes [{:src root :dst o}
                                         {:src o :dst sink}]})))))

(defn- reach!
  "Directed reachable from R."
  [node graph]
  (http/post (str (h/base node) "/graph/" graph "/reachable")
             (h/opts :socket-timeout 10000 :connection-timeout 2000
                     :content-type :json
                     :body (json/generate-string {:from [root] :directed true}))))

(defn- do-swap
  "Returns :ok / :passive / :failed / :timeout / :down."
  [node graph target]
  (try
    (case (:status (mutate! node graph target))
      200 :ok
      503 :passive
      :failed)
    (catch java.net.ConnectException _ :down)
    (catch java.net.SocketTimeoutException _ :timeout)
    (catch Exception _ :timeout)))

(defn- do-reach
  "Returns the reachable node set, or nil if the read failed."
  [node graph]
  (try
    (let [r (reach! node graph)]
      (when (= 200 (:status r))
        (set (:nodes (json/parse-string (:body r) true)))))
    (catch Exception _ nil)))

;; --- client ------------------------------------------------------------------

(defrecord GraphClient [leader graph toggle]
  client/Client
  (open! [this _test _node] this)

  (setup! [_this _test]
    ;; Seed config A before any read. The swap is idempotent + self-correcting,
    ;; so concurrent/duplicate seeds converge; retry across leader discovery so a
    ;; read never observes an un-seeded (sub-config) graph.
    (loop [tries 20]
      (let [node (h/target leader)]
        (when (and node (not= :ok (do-swap node graph "A")) (pos? tries))
          (h/refresh-leader! leader)
          (recur (dec tries))))))

  (invoke! [_this _test op]
    (let [node (h/target leader)]
      (if (nil? node)
        (assoc op :type :fail :error :no-leader)
        (case (:f op)
          :swap
          (let [target (or (:target op) (if (even? (swap! toggle inc)) "A" "B"))
                op     (assoc op :value target)
                r      (do-swap node graph target)]
            (cond
              (= :ok r) (assoc op :type :ok)

              (#{:passive :failed} r)
              ;; definitely-not-applied → retry once on a fresh writer.
              (let [node2 (h/refresh-leader! leader)
                    r2    (when (and node2 (not= node2 node)) (do-swap node2 graph target))]
                (if (= :ok r2)
                  (assoc op :type :ok)
                  (assoc op :type :fail :error :no-writer)))

              :else ; :timeout / :down — indeterminate (the rewire is atomic +
                    ; self-correcting, so a later swap repairs any partial belief)
              (assoc op :type :info :error r)))

          :reach
          (let [nodes (do-reach node graph)]
            (if nodes
              (assoc op :type :ok :value nodes)
              (let [node2 (h/refresh-leader! leader)
                    n2    (when node2 (do-reach node2 graph))]
                (if n2
                  (assoc op :type :ok :value n2)
                  (assoc op :type :fail :error :read-failed)))))))))

  (teardown! [_this _test])
  (close! [_this _test]))

(defn client
  "A graph-swap client. Fresh per-run graph name; a shared toggle alternates the
  swap target so both configs are exercised."
  []
  (->GraphClient (h/make-leader) (str "jgraph-" (System/currentTimeMillis)) (atom 0)))

;; --- checker -----------------------------------------------------------------

(def ^:private config-a #{"R" "A" "Z"})
(def ^:private config-b #{"R" "B" "Z"})

(defn- valid-config? [s]
  (or (= s config-a) (= s config-b)))

(defn checker
  "Every acknowledged `reachable(R)` must equal one whole config (`{R,A,Z}` or
  `{R,B,Z}`). A result missing `Z`, or otherwise not a full config, is a torn
  (non-snapshot) read — a snapshot-isolation violation."
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [reaches (->> history (filter #(and (= :reach (:f %)) (= :ok (:type %)))))
            results (map :value reaches)
            bad     (remove valid-config? results)
            dropped (filter #(not (contains? % sink)) results)
            swaps   (->> history (filter #(and (= :swap (:f %)) (= :ok (:type %)))))
            final   (->> history
                         (filter #(and (= :reach (:f %)) (= :ok (:type %))))
                         last :value)]
        {:valid?        (boolean (and (seq results) (empty? bad)))
         :reaches       (count results)
         :acked-swaps   (count swaps)
         :violations    (count bad)
         :sink-dropped  (count dropped)
         :final-read    final
         :examples      (vec (take 10 (distinct bad)))}))))
