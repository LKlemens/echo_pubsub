(ns jepsen.echo.db
  "Installs and runs the node_app release on each node."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info]]
            [jepsen [control :as c]
                    [db :as db]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]))

(def dir "/opt/echo")
(def binary (str dir "/bin/node_app"))
(def stdout-log (str dir "/stdout.log"))
(def pidfile (str dir "/node_app.pid"))
(def deliveries-log (str dir "/deliveries.log"))
(def port 8080)

(defn node-name
  "The Erlang node name for a Jepsen node, e.g. n1 -> echo@n1."
  [node]
  (str "echo@" node))

(defn- env
  [test node]
  (update-vals
   {:RELEASE_DISTRIBUTION "none"
   :LANG                 "C.UTF-8"
   :PORT                 port
   :JEPSEN_LOG           deliveries-log
   :JEPSEN_NODE_NAME     (node-name node)
   :JEPSEN_NODES         (str/join "," (map node-name (:nodes test)))
   :ECHO_BUFFER_SIZE     (:buffer-size test)
   :ECHO_BATCH_INTERVAL  (:batch-interval test)
   :ECHO_CALL_TIMEOUT    (:call-timeout test)}
   str))

(defn- start-vm!
  [test node]
  (c/su
    ; The release boots with distribution off, so epmd is not started for us.
    (c/exec (c/lit (str "$(ls -d " dir "/erts-*)/bin/epmd -daemon")))
    (cu/start-daemon! {:logfile stdout-log
                       :pidfile pidfile
                       :chdir   dir
                       :env     (env test node)}
                      binary
                      :start)))

(defn- running?
  []
  (try (c/exec :pgrep :beam.smp) true
       (catch Exception _ false)))

(defn start!
  "Starts the release unless a VM is already up. The kill nemesis restarts every
  node, and the pidfile names the launcher script rather than the VM, so a
  second VM would boot beside the first, log a bogus boot entry and crash."
  [test node]
  (when-not (running?)
    (start-vm! test node))
  (cu/await-tcp-port port {:timeout 30000}))

(defn kill!
  [_test _node]
  (c/su
    (cu/grepkill! :kill "beam.smp")
    (c/exec :rm :-f pidfile)))

(defn db
  "The echo_pubsub database: one node_app release per node."
  []
  (reify
    db/DB
    (setup! [_ test node]
      (info "installing node_app on" node)
      (c/su
        (debian/install [:libssl3 :libncurses6 :procps])
        (c/exec :mkdir :-p dir)
        (c/upload (:tarball test) "/tmp/node_app.tar.gz")
        (c/exec :tar :xzf "/tmp/node_app.tar.gz" :-C dir))
      (start! test node))

    (teardown! [_ test node]
      (kill! test node)
      (c/su (c/exec :rm :-rf dir)))

    db/LogFiles
    (log-files [_ _test _node]
      [stdout-log deliveries-log])

    db/Kill
    (start! [_ test node] (start! test node))
    (kill! [_ test node] (kill! test node))

    db/Pause
    (pause! [_ _test _node] (c/su (cu/grepkill! :stop "beam.smp")))
    (resume! [_ _test _node] (c/su (cu/grepkill! :cont "beam.smp")))))
