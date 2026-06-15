(ns bluedb.jepsen.http
  "Thin HTTP layer over the bluedb-server REST API + leader discovery.

  The cluster is the docker-compose stack: three nodes mapped to host ports
  8081/8082/8083. Exactly one node is the active writer at a time (Postgres
  lease); the others are read replicas. We always route through the *current*
  writer so the cluster presents as one logical linearizable KV store whose
  identity moves on failover."
  (:require [clj-http.client :as http]
            [cheshire.core :as json]))

(def ports
  "Logical node name -> host port for the compose stack."
  {"node1" 8081 "node2" 8082 "node3" 8083})

(defn node->container [n] (str "bluedb-" n "-1"))

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
  from an empty table with the required primary key. No-op if no writer is found."
  []
  (when-let [node (active-node)]
    (drop-table! node)
    (create-table! node)
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
