(ns bluedb.jepsen.core-test
  (:require [bluedb.jepsen.core :as core]
            [clojure.test :refer [deftest is]]))

(defn- fs [ops]
  (keep :f ops))

(deftest kubernetes-recovery-avoids-docker-only-operations
  (is (= [:heal :start-all]
         (fs (core/recovery-ops "kubernetes"))))
  (is (not-any? #{:delete-lease :resume-postgres :start-postgres :resume-minio :free-disk :resume}
                (fs (core/recovery-ops "kubernetes")))))

(deftest kubernetes-mix-includes-pod-partition-and-lease-faults
  (let [ops (fs (core/fault-cycle "kubernetes" "mix"))]
    (is (some #{:kill-writer} ops))
    (is (some #{:partition-writer} ops))
    (is (some #{:delete-lease} ops))
    (is (some #{:heal} ops))))

(deftest docker-recovery-keeps-existing-compose-cleanup
  (is (= [:heal :start-all :resume :resume-postgres :start-postgres :resume-minio :free-disk]
         (fs (core/recovery-ops "docker")))))
