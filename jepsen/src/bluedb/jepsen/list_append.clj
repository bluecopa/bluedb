(ns bluedb.jepsen.list-append
  "Elle list-append workload for bluedb.

  Each Jepsen transaction is a list of micro-ops — `[:append k v]` (append unique
  value v to the list at key k) and `[:r k nil]` (read the list at k) — executed
  as ONE SQL transaction:

    BEGIN;
      INSERT INTO la (id,k,v)                                    -- :append
        SELECT <lo> + COALESCE((SELECT COUNT(*) FROM la WHERE id>=<lo> AND id<<hi>), 0), k, v;
      SELECT v FROM la WHERE id>=<lo> AND id<<hi> ORDER BY id;   -- :r
      ...
    COMMIT;

  sent as a single POST /admin/sql (the surface for explicit multi-statement
  transactions — /sql takes exactly one statement) so the whole block runs inside
  one `BEGIN..COMMIT` on the active writer, which holds the write lease for it.
  Elle's list-append checker reconstructs the transaction dependency graph from
  the observed reads and flags any serializability anomaly (G0/G1/G2, lost update,
  write skew, ...).

  Data model (one row per appended element): `la (id INTEGER PRIMARY KEY, k, v)`.
  Why a surrogate `id` rather than the obvious keys:

  * Elle's appended values are unique only *within* a key (value 1 is the first
    element of *every* key's list), so `v` alone can't be the primary key — it
    collides across keys. The natural unique key is the pair (k, position).
  * Under index-organized (PK-clustered) storage a `WHERE k=?` read returns rows
    in primary-key order, not append order, and an `ORDER BY` on a non-PK,
    non-indexed column is rejected by the query guardrail.

  Both are solved by encoding (key, position) into one integer primary key,
  `id = k * KEY-STRIDE + position` (`position` is 0-based within the key, always
  `< KEY-STRIDE`). That id is globally unique, and a key's rows occupy one
  contiguous PK range `[lo, hi) = [k*STRIDE, (k+1)*STRIDE)` *in append order* — so
  the read is a PK-range scan ordered by the PK: no secondary index, no in-memory
  sort.

  `position` is assigned by the *writer*, not the client: it is the current row
  count in the key's id-range at apply time — dense, monotonic in commit order,
  and correct even for two appends to the same key in one txn (the transaction
  overlay makes the second count see the first insert). A client-assigned counter
  would capture *generation* order, which can diverge from commit order under
  concurrency and fabricate cycles. GlueSQL has no auto-increment, so the count is
  read in the same statement via a subquery — a *scalar subquery* under a no-FROM
  outer SELECT, because GlueSQL emits **zero** rows for an aggregate over an empty
  set (a bare `INSERT ... SELECT COUNT(*) FROM ...` would insert nothing on the
  first append to a key). `COALESCE(scalar, 0)` makes the empty case 0 and the
  no-FROM SELECT always yields exactly one row. `lo`/`hi` are computed client-side
  and passed as literals (server-side `k * STRIDE` coerces to float in GlueSQL)."
  (:require [bluedb.jepsen.http :as h]
            [jepsen.client :as client]
            [clojure.string :as str]
            [clj-http.client :as http]
            [cheshire.core :as json]))

;; Reset the list-append table exactly once per JVM/test run (the first client
;; to win the CAS bootstraps it). Schemaless auto-create was removed, so the
;; table must exist before the first append, and it is dropped first so a re-run
;; starts empty.
(defonce ^:private table-ready (atom false))

(def ^:private key-stride
  "Size of each key's primary-key id range. A key's elements get ids
  `k*key-stride + 0,1,2,…`; this must exceed the largest list a key can reach
  (`:max-writes-per-key`, 16 in core.clj) so ranges never overlap. 1e6 is huge
  headroom while keeping ids well inside i64 even for thousands of keys."
  1000000)

(defn- op->sql [[f k v]]
  (let [lo (* k key-stride)
        hi (+ lo key-stride)]          ; this key's contiguous id range [lo, hi)
    (case f
      ;; Append one row. `id = lo + (current count in this key's id-range)` — the
      ;; writer stamps the position; see the ns docstring for the full rationale
      ;; (global uniqueness, empty-aggregate quirk, float coercion of k*stride).
      :append (format (str "INSERT INTO la (id, k, v) "
                           "SELECT %d + COALESCE((SELECT COUNT(*) FROM la WHERE id >= %d AND id < %d), 0), %d, %d;")
                      lo lo hi k v)
      ;; Read the list back in append order: a PK-range scan ordered by the PK.
      :r      (format "SELECT v FROM la WHERE id >= %d AND id < %d ORDER BY id;" lo hi))))

(defn- txn->sql [ops]
  (str "BEGIN; " (str/join " " (map op->sql ops)) " COMMIT;"))

(defn- post-sql
  "POST a multi-statement transaction to `node`'s /admin/sql. The /sql surface
  takes exactly one statement, so an explicit `BEGIN; …; COMMIT;` block (the
  whole point of this workload) must go through /admin/sql — the documented
  surface for DDL / explicit transactions / multi-statement SQL. It runs on the
  same serialized writer connection and through the same query guardrail. The
  body is JSON `{sql}`. Returns the ring response, or throws on connect/timeout."
  [node sql]
  (http/post (str (h/base node) "/admin/sql")
             {:body (json/generate-string {:sql sql})
              :content-type :json
              :throw-exceptions false
              :socket-timeout 8000
              :connection-timeout 2000}))

(defn- fill-reads
  "Map the micro-ops back over the per-statement payloads. `payloads` is the
  JSON array from /admin/sql: [begin, op0, op1, ..., commit], so op i is at index
  i+1. An :append keeps its value; an :r is filled with the list it read."
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

  (setup! [_this _test]
    ;; First client bootstraps `la (id PK, k, v)` on the active writer (the schema
    ;; regime needs an explicit PK'd table; raw DDL over /sql is rejected, so this
    ;; goes through the structured /schema/tables endpoint).
    (when (compare-and-set! table-ready false true)
      (h/reset-la-table!)))

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
