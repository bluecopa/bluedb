(ns bluedb.jepsen.kubernetes-test
  (:require [bluedb.jepsen.kubernetes :as k]
            [clojure.test :refer [deftest is testing]]))

(deftest ready-pods-require-running-ready-pods
  (let [pods {:items [{:metadata {:name "writer-a"}
                       :status {:phase "Running"
                                :conditions [{:type "Ready" :status "True"}]}}
                      {:metadata {:name "starting"}
                       :status {:phase "Running"
                                :conditions [{:type "Ready" :status "False"}]}}
                      {:metadata {:name "done"}
                       :status {:phase "Succeeded"
                                :conditions [{:type "Ready" :status "True"}]}}]}]
    (is (= ["writer-a"] (map :name (k/ready-pods pods))))))

(deftest kubectl-arguments-are-built-from-config
  (let [cfg {:namespace "bluedb"
             :service-port 8080
             :lease-name "bluedb-writer"}]
    (is (= ["-n" "bluedb" "port-forward" "pod/bluedb-0" "18080:8080"]
           (k/port-forward-args cfg "bluedb-0" 18080)))
    (is (= ["-n" "bluedb" "delete" "pod" "bluedb-0" "--wait=false"]
           (k/delete-pod-args cfg "bluedb-0")))
    (is (= ["-n" "bluedb" "delete" "lease" "bluedb-writer" "--ignore-not-found=true"]
           (k/delete-lease-args cfg)))))

(deftest network-policy-denies-isolated-pods
  (let [yaml (k/network-policy-yaml {:namespace "bluedb"
                                     :partition-policy-name "bluedb-jepsen-isolate"})]
    (testing "uses a precise pod label selector"
      (is (re-find #"jepsen\.bluedb\.io/isolated" yaml))
      (is (re-find #"bluedb-jepsen-isolate" yaml)))
    (testing "denies both ingress and egress"
      (is (re-find #"policyTypes:\n  - Ingress\n  - Egress" yaml))
      (is (not (re-find #"ingress:" yaml)))
      (is (not (re-find #"egress:" yaml))))))
