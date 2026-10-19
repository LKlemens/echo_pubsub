(ns jepsen.echo
  "Entry point: `lein run test --workload delivery --nemesis partition,kill,pause`."
  (:require [clojure.string :as str]
            [jepsen [checker :as checker]
                    [cli :as cli]
                    [generator :as gen]
                    [tests :as tests]]
            [jepsen.echo [checker :as echo-checker]
                         [client :as client]
                         [db :as db]]
            [jepsen.nemesis.combined :as nc]
            [jepsen.os.debian :as debian]))

(def workloads
  "Per-workload defaults. `expiry` shrinks the buffer so partitioned nodes fall
  behind it; every gap must then be explained by a cursor_expired."
  {:delivery {:buffer-size 1000000 :expiry-allowed? false}
   :expiry   {:buffer-size 50 :expiry-allowed? true}})

(def all-faults #{:partition :kill :pause})

(defn- broadcasts
  "Broadcast ops with globally unique ids, so a log entry maps to one op."
  []
  (let [next-id (atom 0)]
    (fn [] {:f :broadcast :value (swap! next-id inc)})))

(defn echo-test
  "Builds the Jepsen test map from parsed CLI options."
  [opts]
  (let [workload (workloads (:workload opts))
        opts     (merge workload (into {} (remove (comp nil? val) opts)))
        db       (db/db)
        nemesis  (nc/nemesis-package {:db       db
                                      :nodes    (:nodes opts)
                                      :faults   (:nemesis opts)
                                      :interval (:nemesis-interval opts)
                                      ; Killing every node at once would excuse
                                      ; every loss as a crash and prove nothing.
                                      :kill     {:targets [:one :minority]}})]
    (merge tests/noop-test
           opts
           {:name      (str "echo-" (name (:workload opts))
                            " " (str/join "," (map name (sort (:nemesis opts)))))
            :os        debian/os
            :db        db
            :client    (client/client)
            :nemesis   (:nemesis nemesis)
            :checker   (checker/compose
                         {:perf       (checker/perf {:nemeses (:perf nemesis)})
                          :stats      (checker/stats)
                          :exceptions (checker/unhandled-exceptions)
                          :delivery   (echo-checker/delivery
                                        {:expiry-allowed? (:expiry-allowed? opts)})})
            :generator (gen/phases
                         ; Let every producer register every peer first: a peer
                         ; only gets what is flushed after it registers.
                         (gen/sleep 10)
                         (->> (broadcasts)
                              (gen/stagger (/ (:rate opts)))
                              (gen/nemesis (:generator nemesis))
                              (gen/time-limit (:time-limit opts)))
                         (gen/nemesis (:final-generator nemesis))
                         (gen/log "Healed; waiting for retries to drain")
                         (gen/sleep (:quiet-period opts))
                         (gen/clients (gen/each-thread {:f :read})))})))

(defn- parse-faults
  [s]
  (->> (str/split s #",") (remove str/blank?) (map keyword) set))

(def cli-opts
  [["-w" "--workload NAME" "delivery or expiry"
    :default  :delivery
    :parse-fn keyword
    :validate [workloads (str "one of " (keys workloads))]]
   [nil "--nemesis FAULTS" "Comma-separated faults: partition, kill, pause. Empty for none."
    :default  all-faults
    :parse-fn parse-faults
    :validate [#(every? all-faults %) (str "subset of " all-faults)]]
   [nil "--nemesis-interval SECONDS" "Mean seconds between faults"
    :default  10
    :parse-fn parse-long]
   [nil "--rate HZ" "Broadcasts per second across all clients"
    :default  50
    :parse-fn parse-double]
   [nil "--buffer-size N" "Producer ring buffer size; overrides the workload default"
    :parse-fn parse-long]
   [nil "--batch-interval MS" "Producer batch interval"
    :default  50
    :parse-fn parse-long]
   [nil "--call-timeout MS" "Producer call timeout to remote workers"
    :default  1000
    :parse-fn parse-long]
   [nil "--quiet-period SECONDS" "Wait after healing before the final read"
    :default  15
    :parse-fn parse-long]
   [nil "--tarball PATH" "node_app release tarball to install"
    :default  "resources/node_app.tar.gz"]])

(defn -main
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn echo-test :opt-spec cli-opts})
                   (cli/serve-cmd))
            args))
