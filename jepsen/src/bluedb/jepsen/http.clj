(ns bluedb.jepsen.http
  "Thin HTTP layer over the bluedb-server REST API + leader discovery.

  The cluster is the docker-compose stack: three nodes mapped to host ports
  8081/8082/8083. Exactly one node is the active writer at a time (Postgres
  lease); the others are read replicas. We always route through the *current*
  writer so the cluster presents as one logical linearizable KV store whose
  identity moves on failover."
  (:require [clj-http.client :as http]
            [cheshire.core :as json]))

(def project
  "Compose project name = container/network prefix. Override to drive a second,
  isolated cluster in parallel (e.g. BLUEDB_JEPSEN_PROJECT=bluedb2) so two test
  sessions don't fight over one stack. Defaults to the `docker compose` default."
  (or (System/getenv "BLUEDB_JEPSEN_PROJECT") "bluedb"))

(def ^:private base-port
  "Host port of node1; node2/node3 are the next two. Override with
  BLUEDB_JEPSEN_BASE_PORT to point at a second cluster (e.g. 8091)."
  (or (some-> (System/getenv "BLUEDB_JEPSEN_BASE_PORT") Integer/parseInt) 8081))

(def ports
  "Logical node name -> host port for the compose stack."
  {"node1" base-port "node2" (+ base-port 1) "node3" (+ base-port 2)})

(defn container
  "Container name compose assigns service `svc` in this project (`<project>-svc-1`)."
  [svc]
  (str project "-" svc "-1"))

(def network
  "The default bridge network compose creates for this project (`<project>_default`)."
  (str project "_default"))

(defn node->container [n] (container n))

(defn base [node] (str "http://localhost:" (get ports node)))

(def ^:private short-opts
  {:throw-exceptions false :socket-timeout 2000 :connection-timeout 1500})

(defn status
  "GET /admin/status for one node, or nil if unreachable."
  [node]
  (try
    (let [r (http/get (str (base node) "/admin/status")
                      (assoc short-opts :as :json))]
      (when (= 200 (:status r)) (:body r)))
    (catch Exception _ nil)))

(defn active-node
  "Poll every node's status and return the name of the active writer, or nil."
  []
  (some (fn [n] (when (= "active" (:role (status n))) n)) (keys ports)))

(defn wait-active-node
  "Poll for an active writer for up to `timeout-ms`, returning the node name or
  nil if none appears in time. A per-run table reset MUST target a writer: just
  after the previous run's `kill` faults the cluster can briefly have no writer,
  and a reset that silently no-ops there would leak the prior run's table state
  into this run (e.g. the counter starts non-zero, so reads exceed this run's
  acknowledged increments). Waiting makes back-to-back runs self-isolating."
  ([] (wait-active-node 30000))
  ([timeout-ms]
   (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
     (loop []
       (or (active-node)
           (when (< (System/currentTimeMillis) deadline)
             (Thread/sleep 500)
             (recur)))))))

;; --- leader-aware routing (shared by the workload clients) -----------------

(defn make-leader
  "A fresh shared cell holding the believed-active node name."
  []
  (atom nil))

(defn refresh-leader!
  "Re-discover the active writer and cache it; returns the node name or nil."
  [leader]
  (let [a (active-node)]
    (reset! leader a)
    a))

(defn target
  "The cached active node, discovering one if the cell is empty."
  [leader]
  (or @leader (refresh-leader! leader)))

(defn drop-table!
  "DELETE /schema/tables/jset on `node` (structured DDL). Tolerates a missing
  table; returns the ring response or nil on connection error."
  [node]
  (try
    (http/delete (str (base node) "/schema/tables/jset") short-opts)
    (catch Exception _ nil)))

(defn create-table!
  "POST /schema/tables on `node` to create `jset (v INTEGER PRIMARY KEY)` via
  structured DDL. The schema regime removed schemaless auto-create, so the table
  must exist with a primary key before any insert. Returns the ring response or
  nil on connection error."
  [node]
  (try
    (http/post (str (base node) "/schema/tables")
               (assoc short-opts
                      :content-type :json
                      :body (json/generate-string
                             {:name "jset"
                              :columns [{:name "v" :type "INTEGER" :primary_key true}]})))
    (catch Exception _ nil)))

(defn reset-set-table!
  "Drop + recreate the grow-only-set table on the active writer so each run starts
  from an empty table with the required primary key. Waits for a writer so the
  drop+recreate can't silently no-op and leak the prior run's rows."
  []
  (when-let [node (wait-active-node)]
    (drop-table! node)
    (create-table! node)
    node))

(defn drop-table-named!
  "DELETE /schema/tables/{table} on `node` (structured DDL). Tolerates a missing
  table; returns the ring response or nil on connection error."
  [node table]
  (try
    (http/delete (str (base node) "/schema/tables/" table) short-opts)
    (catch Exception _ nil)))

(defn reset-counter-table!
  "Drop + recreate `cnt (id INTEGER PRIMARY KEY, n INTEGER)` and seed row (1, 0)
  on the active writer, so a counter run starts from a known zero (the schema
  regime needs an explicit PK'd table and no longer auto-creates one).

  Waits for a writer (the cluster may be recovering from a prior run's `kill`)
  and VERIFIES the seed reads back 0 before returning — a silent no-op or an
  unverified seed would leak the prior run's accumulated counter into this run,
  so the checker would see reads above this run's acknowledged increments. Throws
  if it can't establish a zeroed table, so a contaminated run fails fast at setup
  rather than mid-analysis."
  []
  (let [node (or (wait-active-node)
                 (throw (ex-info "counter reset: no active writer appeared" {})))]
    (drop-table-named! node "cnt")
    (http/post (str (base node) "/schema/tables")
               (assoc short-opts
                      :content-type :json
                      :body (json/generate-string
                             {:name "cnt"
                              :columns [{:name "id" :type "INTEGER" :primary_key true}
                                        {:name "n" :type "INTEGER"}]})))
    (http/post (str (base node) "/tables/cnt")
               (assoc short-opts
                      :content-type :json
                      :body (json/generate-string {:id 1 :n 0})))
    ;; Read back through the writer to confirm the seed took and no stale value
    ;; survived the drop/recreate.
    (let [r (http/get (str (base node) "/tables/cnt?id=eq.1")
                      (assoc short-opts :socket-timeout 5000))
          n (when (= 200 (:status r))
              (some-> (json/parse-string (:body r) true) first :n))]
      (when (not= 0 n)
        (throw (ex-info "counter reset: seed did not read back as 0"
                        {:node node :read n}))))
    node))

(defn reset-unique-table!
  "Drop + recreate `u (id INTEGER PRIMARY KEY)` on the active writer so each run
  starts empty (ids are never reused within a run, so the duplicate-insert
  checker stays sound). Waits for a writer so the reset can't silently no-op."
  []
  (when-let [node (wait-active-node)]
    (drop-table-named! node "u")
    (try
      (http/post (str (base node) "/schema/tables")
                 (assoc short-opts
                        :content-type :json
                        :body (json/generate-string
                               {:name "u"
                                :columns [{:name "id" :type "INTEGER" :primary_key true}]})))
      (catch Exception _ nil))
    node))

(defn reset-la-table!
  "Drop + recreate the list-append table on the active writer:
  `la (id INTEGER PRIMARY KEY, k INTEGER, v INTEGER)`. One row per appended
  element. `id` is a surrogate that encodes (key, position) as `k * KEY-STRIDE +
  position`, so it is **globally unique** (Elle's appended values are only unique
  *within* a key, so `v` alone can't be the PK) and, crucially, clusters a key's
  rows into one contiguous primary-key range *in append order*. That makes the
  read a PK-range scan ordered by the PK — no secondary index and no in-memory
  sort, which the guardrail would otherwise reject. Waits for a writer so the
  reset can't silently no-op and leak the prior run's rows."
  []
  (when-let [node (wait-active-node)]
    (drop-table-named! node "la")
    (try
      (http/post (str (base node) "/schema/tables")
                 (assoc short-opts
                        :content-type :json
                        :body (json/generate-string
                               {:name "la"
                                :columns [{:name "id" :type "INTEGER" :primary_key true}
                                          {:name "k" :type "INTEGER"}
                                          {:name "v" :type "INTEGER"}]})))
      (catch Exception _ nil))
    node))

(defn add!
  "POST /tables/jset {v}. Returns the ring response (status code), or throws on
  connection/timeout errors."
  [node v]
  (http/post (str (base node) "/tables/jset")
             (assoc short-opts
                    :socket-timeout 5000
                    :content-type :json
                    :body (json/generate-string {:v v}))))

(defn add-via-sql!
  "Insert `v` into jset through the **/sql autocommit** path (JSON {sql}), i.e.
  the exact write path the counter workload uses — same `SlateDbStorage::commit`
  → `Db::write(await_durable=true)`. Used by the durability probe so a lost write
  is identifiable by its unique `v` (unlike counter's anonymous increments).
  Returns the ring response, or throws on connection/timeout errors."
  [node v]
  (http/post (str (base node) "/sql")
             (assoc short-opts
                    :socket-timeout 5000
                    :content-type :json
                    :body (json/generate-string
                           {:sql (format "INSERT INTO jset (v) VALUES (%d);" v)}))))

(def ^:private page-size
  "Bounded-reads guardrail caps a single read at 100 rows, so the final-read
  phase must paginate over the primary key to reconstruct the whole set."
  100)

(defn read-set
  "Read the entire jset back from `node` via keyset pagination over the primary
  key (`order=v.asc` + `v=gt.<last>`), since one GET is capped at `page-size`
  rows. Returns a sorted-set of ints, or nil if any page fails (the caller
  retries on another node)."
  [node]
  (loop [acc   (sorted-set)
         after nil]
    (let [url (str (base node) "/tables/jset?order=v.asc&limit=" page-size
                   (when after (str "&v=gt." after)))
          r   (http/get url (assoc short-opts :socket-timeout 5000))]
      (when (= 200 (:status r))
        (let [vs (map :v (json/parse-string (:body r) true))]
          (if (< (count vs) page-size)
            (into acc vs)
            (recur (into acc vs) (last vs))))))))

(defn exec-sql!
  "POST /sql raw statement (only succeeds on the active writer). Ignores errors."
  [node sql]
  (try
    (http/post (str (base node) "/sql") (assoc short-opts :body sql))
    (catch Exception _ nil)))
