(ns bluedb.jepsen.ledger
  "Ledger workload — double-entry conservation under faults.

  Drives the `/ledger/*` HTTP API (TigerBeetle-style). A fixed set of accounts
  is created at setup; the generator then issues random posted transfers between
  them and periodic reads. Every transfer is atomic and durable-before-ack, so
  two invariants must hold no matter what the nemesis does (kill/partition/
  pause/skew the writer):

  * **Conservation** — at the final quiescent read, Σ debits_posted = Σ
    credits_posted across all accounts. Each applied transfer adds the same
    `amount` to one account's debits_posted and to another's credits_posted, so
    the totals can only stay equal. A mismatch means a torn or partially-lost
    write.
  * **Accounting bounds (no double-apply / no lost transfer)** — the observed
    total of posted debits is bounded below by the transfers the server
    acknowledged as applied (`created`/`exists`) and above by those plus the
    indeterminate ones (`:info`, e.g. a timeout mid-failover that may or may not
    have committed). Outside that band means a transfer applied twice, or an
    acknowledged one vanished.

  Conservation is checked against the canonical `GET /ledger/accounts/{id}`
  reads (not the SQL projection), so it tests the ledger engine directly.

  Runs are made independent of any persisted cluster state (there is no
  ledger-clear primitive) by namespacing every account and transfer id under a
  per-run `base` offset, so a fresh run never collides with a prior one's ids.

  Example:

    lein run test --workload ledger --nemesis mix --time-limit 120 \\
      --concurrency 10 --node node1 --node node2 --node node3"
  (:require [bluedb.jepsen.http :as h]
            [jepsen.client :as client]
            [jepsen.checker :as checker]
            [clj-http.client :as http]
            [cheshire.core :as json]))

(def ^:private n-accounts 8)
(def ^:private the-ledger 700)

(defn- account-id [base i] (+ base i))
(defn- transfer-id [base k] (+ base 1000 k))

;; --- HTTP --------------------------------------------------------------------

(defn- create-accounts! [node base n]
  (http/post (str (h/base node) "/ledger/accounts")
             {:throw-exceptions false :socket-timeout 8000 :connection-timeout 2000
              :content-type :json
              :body (json/generate-string
                     (mapv (fn [i] {:id (str (account-id base i)) :ledger the-ledger :code 1})
                           (range 1 (inc n))))}))

(defn- post-transfer! [node {:keys [id debit credit amount]}]
  (http/post (str (h/base node) "/ledger/transfers")
             {:throw-exceptions false :socket-timeout 8000 :connection-timeout 2000
              :content-type :json
              :body (json/generate-string
                     [{:id (str id) :debit_account_id (str debit) :credit_account_id (str credit)
                       :amount (str amount) :ledger the-ledger :code 1}])}))

(defn- read-account [node id]
  (let [r (http/get (str (h/base node) "/ledger/accounts/" id)
                    {:throw-exceptions false :socket-timeout 5000 :connection-timeout 2000})]
    (when (= 200 (:status r))
      (let [m (json/parse-string (:body r) true)]
        {:debits_posted  (bigint (:debits_posted m))
         :credits_posted (bigint (:credits_posted m))}))))

(defn- read-all
  "Read every account's posted balances, keyed by id. nil if any read fails."
  [node base n]
  (reduce (fn [acc i]
            (let [id (account-id base i)]
              (if-let [b (read-account node id)]
                (assoc acc id b)
                (reduced nil))))
          {} (range 1 (inc n))))

(defn- do-transfer
  "Returns [:ok <result-keyword>] on a 200 (result is the per-item code), or one
  of :passive / :failed / :timeout / :down."
  [node t]
  (try
    (let [r (post-transfer! node t)]
      (case (:status r)
        200 [:ok (-> (json/parse-string (:body r) true) :results first :result keyword)]
        503 :passive
        :failed))
    (catch java.net.ConnectException _ :down)
    (catch java.net.SocketTimeoutException _ :timeout)
    (catch Exception _ :timeout)))

;; --- client ------------------------------------------------------------------

(defrecord LedgerClient [leader ids base n]
  client/Client
  (open! [this _test _node] this)

  (setup! [_this _test]
    ;; Idempotent: re-creating an existing account returns `exists`.
    (when-let [node (h/target leader)]
      (try (create-accounts! node base n) (catch Exception _ nil))))

  (invoke! [_this _test op]
    (let [node (h/target leader)]
      (if (nil? node)
        (assoc op :type :fail :error :no-leader)
        (case (:f op)
          :transfer
          (let [{:keys [from to amount]} (:value op)
                t {:id (transfer-id base (swap! ids inc))
                   :debit (account-id base from) :credit (account-id base to) :amount amount}
                op (assoc op :value t)
                r (do-transfer node t)]
            (cond
              (and (vector? r) (= :ok (first r)))
              (if (#{:created :exists} (second r))
                (assoc op :type :ok :result (second r))
                ;; A deterministic reject (200 + non-created code): not applied.
                (assoc op :type :fail :result (second r)))

              (#{:passive :failed} r)
              ;; The node refused (definitely not applied) — retry once on a
              ;; freshly discovered writer with the SAME id (idempotent).
              (let [node2 (h/refresh-leader! leader)
                    r2 (when (and node2 (not= node2 node)) (do-transfer node2 t))]
                (if (and (vector? r2) (= :ok (first r2)) (#{:created :exists} (second r2)))
                  (assoc op :type :ok :result (second r2))
                  (assoc op :type :fail :error :no-writer)))

              :else ; :timeout / :down — indeterminate (may or may not have applied)
              (assoc op :type :info :error r)))

          :read
          (let [bal (try (read-all node base n) (catch Exception _ nil))]
            (if bal
              (assoc op :type :ok :value bal)
              (let [node2 (h/refresh-leader! leader)
                    bal2 (when node2 (try (read-all node2 base n) (catch Exception _ nil)))]
                (if bal2
                  (assoc op :type :ok :value bal2)
                  (assoc op :type :fail :error :read-failed)))))))))

  (teardown! [_this _test])
  (close! [_this _test]))

(defn client
  "A ledger client. `base` namespaces this run's ids so runs are independent of
  any persisted cluster state."
  []
  (let [base (* (System/currentTimeMillis) 100000)]
    (->LedgerClient (h/make-leader) (atom 0) base n-accounts)))

;; --- checker -----------------------------------------------------------------

(defn checker
  "Conservation + accounting-bounds over the final quiescent read."
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [done      (filter #(and (= :transfer (:f %)) (#{:ok :info :fail} (:type %))) history)
            amount-of (fn [op] (bigint (:amount (:value op))))
            applied   (->> done
                           (filter #(and (= :ok (:type %)) (#{:created :exists} (:result %))))
                           (map amount-of)
                           (reduce + 0N))
            indet     (->> done
                           (filter #(= :info (:type %)))
                           (map amount-of)
                           (reduce + 0N))
            final     (->> history
                           (filter #(and (= :read (:f %)) (= :ok (:type %))))
                           last
                           :value)
            obs-d     (reduce + 0N (map :debits_posted (vals final)))
            obs-c     (reduce + 0N (map :credits_posted (vals final)))
            conserved (= obs-d obs-c)
            in-bounds (and (<= applied obs-d) (<= obs-d (+ applied indet)))]
        {:valid?            (boolean (and (some? final) conserved in-bounds))
         :conserved?        conserved
         :in-bounds?        in-bounds
         :observed-debits   obs-d
         :observed-credits  obs-c
         :acknowledged-sum  applied
         :indeterminate-sum indet
         :final-read?       (some? final)}))))
