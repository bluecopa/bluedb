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
    :reset-clock       restore every node's clock."
  (:require [bluedb.jepsen.http :as h]
            [jepsen.nemesis :as nemesis]
            [clojure.java.shell :as shell]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info]]))

(def ^:private net "bluedb_default")

(defn- docker [& args]
  (let [{:keys [exit out err]} (apply shell/sh "docker" args)]
    {:exit exit :out (str/trim (str out)) :err (str/trim (str err))}))

(defn- containers [] (map h/node->container (keys h/ports)))

(defn nemesis
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
              (assoc op :value :clock-reset))))

      (teardown! [_this _test]
        ;; best-effort: bring everything back so the cluster is usable after the run
        (doseq [c (containers)]
          (docker "start" c)
          (docker "network" "connect" net c)
          (docker "exec" c "sh" "-c" "echo '+0' > /faketime/offset"))))))
