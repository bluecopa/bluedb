(defproject bluedb-jepsen "0.1.0"
  :description "Jepsen test for bluedb: single-writer KV durability across kill/partition failover"
  :url "https://github.com/bluecopa/bluedb"
  :main bluedb.jepsen.core
  :jvm-opts ["-Djava.awt.headless=true"
             ;; JDK 17 needs these opened for Jepsen's transitive deps
             "--add-opens=java.base/java.lang=ALL-UNNAMED"
             "--add-opens=java.base/java.util=ALL-UNNAMED"
             "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED"
             "--add-opens=java.base/java.io=ALL-UNNAMED"]
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [jepsen "0.3.5"]
                 [clj-http "3.12.3"]
                 [cheshire "5.11.0"]])
