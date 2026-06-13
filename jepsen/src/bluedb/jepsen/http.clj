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

(defn add!
  "POST /tables/jset {v}. Returns the ring response (status code), or throws on
  connection/timeout errors."
  [node v]
  (http/post (str (base node) "/tables/jset")
             (assoc short-opts
                    :socket-timeout 5000
                    :content-type :json
                    :body (json/generate-string {:v v}))))

(defn read-set
  "GET /tables/jset on any node (replicas serve reads too). Returns a sorted-set
  of ints, or nil on failure."
  [node]
  (let [r (http/get (str (base node) "/tables/jset?order=v.asc")
                    (assoc short-opts :socket-timeout 5000))]
    (when (= 200 (:status r))
      (into (sorted-set) (map :v (json/parse-string (:body r) true))))))

(defn exec-sql!
  "POST /sql raw statement (only succeeds on the active writer). Ignores errors."
  [node sql]
  (try
    (http/post (str (base node) "/sql") (assoc short-opts :body sql))
    (catch Exception _ nil)))
