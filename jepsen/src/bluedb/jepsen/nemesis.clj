(ns bluedb.jepsen.nemesis
  "Docker-driven faults against the compose cluster. We don't SSH into nodes
  (the cluster is already up); instead the control process shells out to the
  `docker` CLI on the host, which is the natural fault-injection seam for a
  containerized deployment.

    :kill-writer       hard-kill the active writer's container (crash). Tests
                       durability of acked writes through SlateDB's WAL window
                       and that a standby promotes.
    :start-all         restart any stopped containers (recover from kill).
    :partition-writer  cut the active writer off the compose network, isolating
                       it from Postgres (lease), MinIO (storage), and peers.
                       Tests clean failover with no split-brain.
    :heal              reconnect every node to the network.
    :skew-clock        shift the active writer's wall clock 8s BACKWARD (via
                       libfaketime; < TTL 10s, > margin 3s). The writer then
                       believes its lease is valid longer than Postgres records,
                       so a standby can acquire concurrently — tests that the
                       SlateDB writer_epoch fence still prevents divergent writes.
    :reset-clock       restore every node's clock.
    :pause-writer      SIGSTOP the active writer's container (docker pause): it
                       stops renewing without crashing. A standby promotes; on
                       :resume the frozen writer wakes to an expired lease and a
                       bumped epoch, so it must step down (fenced).
    :resume            unpause every container.
    :isolate-half      asymmetric partition: cut the active writer AND one peer
                       off the network (a 2-node minority that loses the Postgres
                       arbiter + MinIO), leaving a lone node + infra to lead.
                       Healed by :heal.

  Faults on the dependencies bluedb relies on (the cluster should stay CONSISTENT
  — no split-brain, no lost acked writes — while losing availability):
    :pause-postgres / :resume-postgres  freeze/thaw the lease arbiter. The writer
                       can't renew (self-fences after TTL) and standbys can't
                       acquire, so the cluster goes writer-less until thaw.
    :pause-minio / :resume-minio  freeze/thaw the object store. The writer keeps
                       its lease but can't durably write, so writes don't ack.
    :fill-disk / :free-disk  fill MinIO's (size-bounded tmpfs) data dir so PUTs
                       fail with ENOSPC, then free it."
  (:require [bluedb.jepsen.http :as h]
            [bluedb.jepsen.kubernetes :as k8s]
            [jepsen.nemesis :as nemesis]
            [clojure.java.shell :as shell]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info]]))

;; Derived from the compose project (h/project) so a second, alternately-named
;; cluster (BLUEDB_JEPSEN_PROJECT=bluedb2) is targeted correctly.
(def ^:private net h/network)
(def ^:private pg-container (h/container "postgres"))
(def ^:private minio-container (h/container "minio"))
(def ^:private filler "/data/.jepsen-filler")
(def ^:private k8s-active-timeout-ms 120000)

(defn- docker [& args]
  (let [{:keys [exit out err]} (apply shell/sh "docker" args)]
    {:exit exit :out (str/trim (str out)) :err (str/trim (str err))}))

(defn- containers [] (map h/node->container (keys h/ports)))

(defn docker-nemesis
  "A custom nemesis driving docker faults. Tracks the container it isolated so
  :heal/teardown can reliably restore connectivity."
  []
  (let [partitioned (atom nil)]
    (reify nemesis/Nemesis
      (setup! [this _test] this)

      (invoke! [_this _test op]
        (case (:f op)
          :kill-writer
          (let [w (h/active-node)]
            (when w (docker "kill" (h/node->container w)))
            (info "nemesis killed writer" w)
            (assoc op :value (str "killed " w)))

          :start-all
          (do (doseq [c (containers)] (docker "start" c))
              (assoc op :value :started))

          :partition-writer
          (let [w (h/active-node)]
            (when w
              (docker "network" "disconnect" net (h/node->container w))
              (reset! partitioned (h/node->container w)))
            (info "nemesis isolated writer" w)
            (assoc op :value (str "isolated " w)))

          :heal
          (do (doseq [c (containers)] (docker "network" "connect" net c))
              (reset! partitioned nil)
              (assoc op :value :healed))

          :skew-clock
          (let [w (h/active-node)]
            (when w
              (docker "exec" (h/node->container w) "sh" "-c" "echo '-8s' > /faketime/offset"))
            (info "nemesis skewed writer clock -8s" w)
            (assoc op :value (str "skewed " w " -8s")))

          :reset-clock
          (do (doseq [c (containers)]
                (docker "exec" c "sh" "-c" "echo '+0' > /faketime/offset"))
              (assoc op :value :clock-reset))

          :pause-writer
          (let [w (h/active-node)]
            (when w (docker "pause" (h/node->container w)))
            (info "nemesis paused writer" w)
            (assoc op :value (str "paused " w)))

          :resume
          (do (doseq [c (containers)] (docker "unpause" c))
              (assoc op :value :resumed))

          :isolate-half
          (let [w       (h/active-node)
                buddy   (first (sort (remove #{w} (keys h/ports))))
                victims (filter some? [w buddy])]
            (doseq [n victims]
              (docker "network" "disconnect" net (h/node->container n)))
            (reset! partitioned victims)
            (info "nemesis isolated minority" victims)
            (assoc op :value (str "isolated " (vec victims))))

          ;; --- dependency faults (arbiter / storage / disk) ---
          :pause-postgres  (do (docker "pause" pg-container)
                               (info "nemesis paused Postgres (lease arbiter)")
                               (assoc op :value :postgres-paused))
          :resume-postgres (do (docker "unpause" pg-container) (assoc op :value :postgres-resumed))
          ;; Hard restart (vs pause): kills the TCP connection, so recovery
          ;; depends on PostgresLeaseProvider reconnecting.
          :stop-postgres   (do (docker "stop" pg-container)
                               (info "nemesis STOPPED Postgres (connection loss)")
                               (assoc op :value :postgres-stopped))
          :start-postgres  (do (docker "start" pg-container) (assoc op :value :postgres-started))
          :pause-minio     (do (docker "pause" minio-container)
                               (info "nemesis paused MinIO (object store)")
                               (assoc op :value :minio-paused))
          :resume-minio    (do (docker "unpause" minio-container) (assoc op :value :minio-resumed))
          :fill-disk       (let [r (docker "exec" minio-container "sh" "-c"
                                           (str "dd if=/dev/zero of=" filler " bs=1M count=4096 2>/dev/null; true"))]
                             (info "nemesis filled MinIO disk" (:err r))
                             (assoc op :value :disk-filled))
          :free-disk       (do (docker "exec" minio-container "sh" "-c" (str "rm -f " filler "; true"))
                               (assoc op :value :disk-freed))))

      (teardown! [_this _test]
        ;; best-effort: bring everything back so the cluster is usable after the
        ;; run — unpause first (a paused container can't exec), then reconnect,
        ;; reset clocks, thaw infra, and free the disk.
        (doseq [c (containers)] (docker "unpause" c))
        (doseq [c (containers)]
          (docker "start" c)
          (docker "network" "connect" net c)
          (docker "exec" c "sh" "-c" "echo '+0' > /faketime/offset"))
        (docker "unpause" pg-container)
        (docker "start" pg-container)
        (docker "unpause" minio-container)
        (docker "exec" minio-container "sh" "-c" (str "rm -f " filler "; true"))))))

(defn- node-order []
  (vec (sort (keys h/ports))))

(defn setup-target!
  "Prepare the requested target backend before DB/client setup touches HTTP."
  [opts]
  (when (= "kubernetes" (:nemesis-backend opts "docker"))
    (k8s/ensure! (k8s/config opts) (node-order) h/ports)
    (or (h/wait-active-node k8s-active-timeout-ms)
        (throw (ex-info "Timed out waiting for an active BlueDB writer"
                        {:backend :kubernetes
                         :timeout-ms k8s-active-timeout-ms})))))

(defn- sync-k8s! [cfg]
  (k8s/sync-port-forwards! cfg (node-order) h/ports))

(defn- wait-k8s-active! []
  (or (h/wait-active-node k8s-active-timeout-ms)
      (throw (ex-info "Timed out waiting for an active BlueDB writer"
                      {:backend :kubernetes
                       :timeout-ms k8s-active-timeout-ms}))))

(defn k8s-nemesis
  "A Kubernetes nemesis for live clusters. It targets individual pods via the
  port-forward registry, so Jepsen can observe leadership movement across pods."
  [opts]
  (let [cfg (k8s/config opts)]
    (reify nemesis/Nemesis
      (setup! [this _test]
        (sync-k8s! cfg)
        (wait-k8s-active!)
        this)

      (invoke! [_this _test op]
        (case (:f op)
          :kill-writer
          (let [node (h/active-node)
                pod  (when node (k8s/node->pod node))]
            (when pod (k8s/delete-pod! cfg pod))
            (info "k8s nemesis deleted writer pod" pod "node" node)
            (assoc op :value (str "deleted " pod)))

          :start-all
          (do (sync-k8s! cfg)
              (wait-k8s-active!)
              (assoc op :value :synced))

          :delete-lease
          (do (k8s/delete-lease! cfg)
              (info "k8s nemesis deleted Lease" (:lease-name cfg))
              (assoc op :value :lease-deleted))

          :partition-writer
          (let [node (h/active-node)
                pod  (when node (k8s/node->pod node))]
            (when pod (k8s/partition-pods! cfg [pod]))
            (info "k8s nemesis isolated writer pod" pod "node" node)
            (assoc op :value (str "isolated " pod)))

          :isolate-half
          (let [node   (h/active-node)
                writer (when node (k8s/node->pod node))
                buddy  (some->> (node-order)
                                (remove #{node})
                                first
                                k8s/node->pod)
                pods   (filterv some? [writer buddy])]
            (when (seq pods) (k8s/partition-pods! cfg pods))
            (info "k8s nemesis isolated pod minority" pods)
            (assoc op :value (str "isolated " pods)))

          :heal
          (do (k8s/heal! cfg)
              (sync-k8s! cfg)
              (wait-k8s-active!)
              (assoc op :value :healed))))

      (teardown! [_this _test]
        (k8s/heal! cfg)
        (sync-k8s! cfg)))))

(defn nemesis [opts]
  (case (:nemesis-backend opts "docker")
    "kubernetes" (k8s-nemesis opts)
    (docker-nemesis)))
