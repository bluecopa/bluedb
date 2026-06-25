(ns bluedb.jepsen.evidence
  "Evidence chain-integrity workload — append durability + dense gap-free seq
  under faults.

  Drives the `/evidence/*` HTTP API. Every client appends an event carrying a
  globally-unique value to ONE fresh per-run chain; the single writer assigns
  each a dense, gap-free `seq`, durable-before-ack. No matter what the nemesis
  does (kill / partition / pause / skew the writer, forcing failover), at the
  final quiescent read three invariants must hold:

  * **No acked loss** — every append the server acknowledged (`:ok`) is present
    in the final full-chain read. An acknowledged value that vanished is a lost
    durable write.
  * **Accounting bounds** — the set of values observed in the final read lies
    within `acked ⊆ observed ⊆ acked ∪ indeterminate`. A value outside that band
    is either fabricated or an acked one that vanished. (`:info` = a timeout mid-
    failover that may or may not have committed.)
  * **Dense, gap-free, monotonic seq** — the final read's seqs are exactly
    `1..N`, unique and contiguous. A gap, duplicate, or reuse means the server-
    assigned sequence broke under faults (the R1 invariant).

  Run-independent: each run targets a fresh chain `jepsen-<ms>` (no chain-clear
  primitive exists), so a run never collides with persisted state from a prior.
  Appends carry NO `idem_key` — on a definitely-not-applied refusal (503/refused)
  the client retries once on a freshly-discovered writer with the SAME value;
  on an indeterminate timeout it does NOT retry (it would risk a double-apply)
  and records `:info`. (An idem_key variant that retries timeouts safely and
  checks no-double-apply is a documented follow-on.)

  Example:
    lein run test --workload evidence --nemesis mix --time-limit 120 \\
      --concurrency 10 --node node1 --node node2 --node node3

  ✅ VALIDATED (2026-06-16) on the live 3-node docker cluster — all green
  (`:valid? true`, zero acked appends lost, seq dense/gap-free 1..N):
    none      — 1023 appends.
    kill      — 1814 appends across 29 crash/restart ops (~15 writer failovers);
                17 indeterminate, all within the safe band.
    partition — 1462 appends across 29 isolate/heal ops; 39 indeterminate, bounded.
    mix       — 2721 appends (kill+partition+pause cycled); 36 indeterminate.
  Leadership moved across 22 epochs during the matrix and the server-assigned
  seq stayed gap-free throughout — durability-before-ack + dense sequencing
  survive crash / partition / pause + failover. (`lein check` clean on Java 21.)"
  (:require [bluedb.jepsen.http :as h]
            [jepsen.client :as client]
            [jepsen.checker :as checker]
            [clojure.set :as set]
            [clj-http.client :as http]
            [cheshire.core :as json])
  (:import (java.util Base64)))

(defn- b64 [^String s]
  (.encodeToString (Base64/getEncoder) (.getBytes s "UTF-8")))
(defn- unb64 [^String s]
  (String. (.decode (Base64/getDecoder) s) "UTF-8"))

;; --- HTTP --------------------------------------------------------------------

(defn- append! [node chain v]
  (http/post (str (h/base node) "/evidence/" chain "/entries")
             (h/opts :socket-timeout 8000 :connection-timeout 2000
                     :content-type :json
                     :body (json/generate-string
                            {:events [{:type "j" :payload_b64 (b64 (str v))}]}))))

(defn- read-chain
  "Full chain read → vector of {:seq n :value v}, or nil if the read fails."
  [node chain]
  (let [r (http/get (str (h/base node) "/evidence/" chain "/entries")
                    (h/opts :socket-timeout 10000 :connection-timeout 2000))]
    (when (= 200 (:status r))
      (->> (json/parse-string (:body r) true)
           (mapv (fn [e] {:seq (:seq e)
                          :value (Long/parseLong (unb64 (:payload_b64 e)))}))))))

(defn- do-append
  "Returns [:ok seqs] on a 200, or :passive / :failed / :timeout / :down."
  [node chain v]
  (try
    (let [r (append! node chain v)]
      (case (:status r)
        ;; 200 = committed. If the seqs are somehow unreadable, treat as
        ;; indeterminate (it *did* apply — must not under-count it).
        200 (if-let [seqs (-> (json/parse-string (:body r) true) :seqs)]
              [:ok seqs]
              :timeout)
        503 :passive
        :failed))
    (catch java.net.ConnectException _ :down)
    (catch java.net.SocketTimeoutException _ :timeout)
    ;; Catch-all errs toward indeterminate (:info), which only widens the safe
    ;; band — never narrows it into a false pass.
    (catch Exception _ :timeout)))

;; --- client ------------------------------------------------------------------

(defrecord EvidenceClient [leader ids chain]
  client/Client
  (open! [this _test _node] this)
  (setup! [_this _test])

  (invoke! [_this _test op]
    (let [node (h/target leader)]
      (if (nil? node)
        (assoc op :type :fail :error :no-leader)
        (case (:f op)
          :append
          (let [v  (swap! ids inc)
                op (assoc op :value v)
                r  (do-append node chain v)]
            (cond
              (and (vector? r) (= :ok (first r)))
              (assoc op :type :ok :seqs (second r))

              (#{:passive :failed} r)
              ;; definitely-not-applied → retry once on a fresh writer, same v.
              (let [node2 (h/refresh-leader! leader)
                    r2    (when (and node2 (not= node2 node)) (do-append node2 chain v))]
                (if (and (vector? r2) (= :ok (first r2)))
                  (assoc op :type :ok :seqs (second r2))
                  (assoc op :type :fail :error :no-writer)))

              :else ; :timeout / :down — indeterminate (may or may not have applied)
              (assoc op :type :info :error r)))

          :read
          (let [entries (try (read-chain node chain) (catch Exception _ nil))]
            (if entries
              (assoc op :type :ok :value entries)
              (let [node2 (h/refresh-leader! leader)
                    e2    (when node2 (try (read-chain node2 chain) (catch Exception _ nil)))]
                (if e2
                  (assoc op :type :ok :value e2)
                  (assoc op :type :fail :error :read-failed)))))))))

  (teardown! [_this _test])
  (close! [_this _test]))

(defn client
  "An evidence client. A fresh per-run chain name makes the run independent of
  persisted cluster state; the shared `ids` atom assigns globally-unique values
  across all concurrent processes so the checker can match acked appends to the
  final read."
  []
  (->EvidenceClient (h/make-leader) (atom 0) (str "jepsen-" (System/currentTimeMillis))))

;; --- checker -----------------------------------------------------------------

(defn checker
  "No-acked-loss + accounting-bounds + dense/gap-free/unique seq over the final
  quiescent full-chain read."
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [appends (filter #(= :append (:f %)) history)
            acked   (->> appends (filter #(= :ok (:type %)))   (map :value) set)
            indet   (->> appends (filter #(= :info (:type %))) (map :value) set)
            final   (->> history
                         (filter #(and (= :read (:f %)) (= :ok (:type %))))
                         last :value)
            fvals   (set (map :value final))
            fseqs   (sort (map :seq final))
            n       (count final)
            dense   (= (seq fseqs) (seq (range 1 (inc n))))
            uniq    (= (count fvals) n)
            no-loss (set/subset? acked fvals)
            bounds  (set/subset? fvals (set/union acked indet))]
        {:valid?        (boolean (and (some? final) no-loss bounds dense uniq))
         :final-read?   (some? final)
         :no-loss?      no-loss
         :in-bounds?    bounds
         :dense-seq?    dense
         :unique?       uniq
         :entries       n
         :acked         (count acked)
         :indeterminate (count indet)
         :lost          (sort (set/difference acked fvals))}))))
