(defproject jepsen.echo "0.1.0-SNAPSHOT"
  :description "Jepsen tests for echo_pubsub's at-least-once delivery"
  :license {:name "MIT"}
  :main jepsen.echo
  :jvm-opts ["-Djava.awt.headless=true"]
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [jepsen "0.3.7"]
                 [clj-http "3.12.3"]
                 [cheshire "5.12.0"]])
