(ns bluedb.jepsen.kubernetes
  "Kubernetes target support for the Jepsen harness.

  The HTTP clients still talk to localhost ports. In Kubernetes mode those ports
  are long-lived `kubectl port-forward pod/<pod>` processes, one per Jepsen
  logical node. The nemesis drives faults with kubectl: deleting the active
  writer pod, deleting the Lease object, and applying a deny-all NetworkPolicy
  to specifically-labelled pods."
  (:require [cheshire.core :as json]
            [clojure.java.io :as io]
            [clojure.java.shell :as shell]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]])
  (:import (java.net InetSocketAddress Socket)
           (java.util.concurrent TimeUnit)))

(def isolated-label "jepsen.bluedb.io/isolated")

(defonce ^:private forwards (atom {}))

(defn- env [k default]
  (let [v (System/getenv k)]
    (if (seq v) v default)))

(defn- parse-int [v default]
  (cond
    (integer? v) v
    (string? v)  (Integer/parseInt v)
    (nil? v)     default
    :else        (Integer/parseInt (str v))))

(defn config
  "Build Kubernetes nemesis config from Jepsen CLI opts and environment."
  [opts]
  {:namespace             (or (:k8s-namespace opts) (env "BLUEDB_JEPSEN_K8S_NAMESPACE" "bluedb"))
   :selector              (or (:k8s-selector opts) (env "BLUEDB_JEPSEN_K8S_SELECTOR" "app.kubernetes.io/name=bluedb"))
   :deployment            (or (:k8s-deployment opts) (env "BLUEDB_JEPSEN_K8S_DEPLOYMENT" "bluedb"))
   :service-port          (parse-int (or (:k8s-port opts) (System/getenv "BLUEDB_JEPSEN_K8S_PORT")) 8080)
   :lease-name            (or (:k8s-lease opts) (env "BLUEDB_JEPSEN_K8S_LEASE" "bluedb-writer"))
   :replicas              (parse-int (or (:k8s-replicas opts) (System/getenv "BLUEDB_JEPSEN_K8S_REPLICAS")) 0)
   :partition-policy-name (or (:k8s-partition-policy opts)
                              (env "BLUEDB_JEPSEN_K8S_PARTITION_POLICY" "bluedb-jepsen-isolate"))})

(defn- kubectl-result [& args]
  (let [{:keys [exit out err]} (apply shell/sh "kubectl" args)]
    {:exit exit :out (str/trim (str out)) :err (str/trim (str err))}))

(defn- kubectl-input [input & args]
  (let [{:keys [exit out err]} (apply shell/sh "kubectl" (concat args [:in input]))]
    {:exit exit :out (str/trim (str out)) :err (str/trim (str err))}))

(defn- kubectl! [& args]
  (let [r (apply kubectl-result args)]
    (when-not (zero? (:exit r))
      (throw (ex-info "kubectl failed" {:args args :result r})))
    r))

(defn port-forward-args [cfg pod local-port]
  ["-n" (:namespace cfg) "port-forward" (str "pod/" pod)
   (str local-port ":" (:service-port cfg))])

(defn delete-pod-args [cfg pod]
  ["-n" (:namespace cfg) "delete" "pod" pod "--wait=false"])

(defn delete-lease-args [cfg]
  ["-n" (:namespace cfg) "delete" "lease" (:lease-name cfg) "--ignore-not-found=true"])

(defn network-policy-yaml [cfg]
  (format (str "apiVersion: networking.k8s.io/v1\n"
               "kind: NetworkPolicy\n"
               "metadata:\n"
               "  name: %s\n"
               "  namespace: %s\n"
               "spec:\n"
               "  podSelector:\n"
               "    matchLabels:\n"
               "      %s: \"true\"\n"
               "  policyTypes:\n"
               "  - Ingress\n"
               "  - Egress\n")
          (:partition-policy-name cfg)
          (:namespace cfg)
          isolated-label))

(defn- ready? [pod]
  (and (= "Running" (get-in pod [:status :phase]))
       (some #(and (= "Ready" (:type %)) (= "True" (:status %)))
             (get-in pod [:status :conditions]))))

(defn ready-pods [pods-json]
  (->> (:items pods-json)
       (filter ready?)
       (map (fn [pod]
              {:name   (get-in pod [:metadata :name])
               :labels (get-in pod [:metadata :labels])}))
       (sort-by :name)
       vec))

(defn list-ready-pods [cfg]
  (let [r (kubectl! "-n" (:namespace cfg)
                    "get" "pods"
                    "-l" (:selector cfg)
                    "-o" "json")]
    (ready-pods (json/parse-string (:out r) true))))

(defn wait-ready-pods [cfg expected timeout-ms]
  (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
    (loop []
      (let [pods (list-ready-pods cfg)]
        (cond
          (>= (count pods) expected) pods
          (< (System/currentTimeMillis) deadline)
          (do (Thread/sleep 1000) (recur))
          :else
          (throw (ex-info "Timed out waiting for ready BlueDB pods"
                          {:expected expected :ready (mapv :name pods)})))))))

(defn- port-open? [port]
  (try
    (with-open [s (Socket.)]
      (.connect s (InetSocketAddress. "127.0.0.1" (int port)) 250)
      true)
    (catch Exception _ false)))

(defn- wait-port [port timeout-ms]
  (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
    (loop []
      (cond
        (port-open? port) true
        (< (System/currentTimeMillis) deadline) (do (Thread/sleep 100) (recur))
        :else false))))

(defn- drain-process! [^Process proc label]
  (future
    (try
      (with-open [rdr (io/reader (.getInputStream proc))]
        (doseq [line (line-seq rdr)]
          (info label line)))
      (catch Exception e
        (warn e "port-forward output drain failed" label)))))

(defn- stop-forward! [{:keys [^Process process]}]
  (when (and process (.isAlive process))
    (.destroy process)
    (when-not (.waitFor process 2 TimeUnit/SECONDS)
      (.destroyForcibly process))))

(defn stop-all-forwards! []
  (doseq [f (vals @forwards)] (stop-forward! f))
  (reset! forwards {}))

(defn- start-forward! [cfg node pod local-port]
  (let [args    (port-forward-args cfg pod local-port)
        process (.start (doto (ProcessBuilder. ^java.util.List (vec (into ["kubectl"] args)))
                          (.redirectErrorStream true)))]
    (drain-process! process (str "k8s-port-forward " node "/" pod))
    (when-not (wait-port local-port 10000)
      (stop-forward! {:process process})
      (throw (ex-info "Timed out waiting for kubectl port-forward"
                      {:node node :pod pod :local-port local-port :args args})))
    {:node node :pod pod :port local-port :process process}))

(defn- alive-forward? [state]
  (and (:process state) (.isAlive ^Process (:process state))))

(defn- assign-pods [nodes pods current]
  (let [ready-names (set (map :name pods))
        preserved   (into {}
                          (keep (fn [node]
                                  (let [pod (:pod (get current node))]
                                    (when (contains? ready-names pod)
                                      [node pod]))))
                          nodes)
        used        (set (vals preserved))
        available   (atom (remove used (map :name pods)))]
    (reduce (fn [m node]
              (if (contains? m node)
                m
                (if-let [pod (first @available)]
                  (do (swap! available rest) (assoc m node pod))
                  m)))
            preserved
            nodes)))

(defn sync-port-forwards!
  "Ensure every logical Jepsen node has a live port-forward to a ready pod."
  [cfg nodes ports]
  (let [pods        (wait-ready-pods cfg (count nodes) 120000)
        assignments (assign-pods nodes pods @forwards)]
    (doseq [node nodes]
      (let [pod  (get assignments node)
            port (get ports node)
            old  (get @forwards node)]
        (when (nil? pod)
          (throw (ex-info "No ready pod available for Jepsen node" {:node node})))
        (when-not (and old (= pod (:pod old)) (= port (:port old)) (alive-forward? old))
          (when old (stop-forward! old))
          (swap! forwards assoc node (start-forward! cfg node pod port)))))
    @forwards))

(defn scale! [cfg replicas]
  (when (pos? replicas)
    (info "scaling bluedb deployment for k8s Jepsen" (:deployment cfg) replicas)
    (kubectl! "-n" (:namespace cfg)
              "scale" (str "deployment/" (:deployment cfg))
              (str "--replicas=" replicas))
    (kubectl! "-n" (:namespace cfg)
              "rollout" "status" (str "deployment/" (:deployment cfg))
              "--timeout=180s")))

(defn ensure!
  "Make sure the Kubernetes target has enough pods and local node port-forwards."
  [cfg nodes ports]
  (scale! cfg (:replicas cfg))
  (sync-port-forwards! cfg nodes ports))

(defn node->pod [node]
  (:pod (get @forwards node)))

(defn pod->node [pod]
  (some (fn [[node state]] (when (= pod (:pod state)) node)) @forwards))

(defn delete-pod! [cfg pod]
  (apply kubectl-result (delete-pod-args cfg pod)))

(defn delete-lease! [cfg]
  (apply kubectl-result (delete-lease-args cfg)))

(defn apply-network-policy! [cfg]
  (kubectl-input (network-policy-yaml cfg)
                 "-n" (:namespace cfg) "apply" "-f" "-"))

(defn delete-network-policy! [cfg]
  (kubectl-result "-n" (:namespace cfg)
                  "delete" "networkpolicy" (:partition-policy-name cfg)
                  "--ignore-not-found=true"))

(defn label-pod! [cfg pod]
  (kubectl-result "-n" (:namespace cfg)
                  "label" "pod" pod (str isolated-label "=true")
                  "--overwrite"))

(defn clear-isolation-labels! [cfg]
  (kubectl-result "-n" (:namespace cfg)
                  "label" "pods" "-l" (str isolated-label "=true")
                  (str isolated-label "-") "--overwrite"))

(defn partition-pods! [cfg pods]
  (doseq [pod pods] (label-pod! cfg pod))
  (apply-network-policy! cfg))

(defn heal! [cfg]
  (delete-network-policy! cfg)
  (clear-isolation-labels! cfg))
