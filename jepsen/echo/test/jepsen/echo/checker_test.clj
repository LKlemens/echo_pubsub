(ns jepsen.echo.checker-test
  (:require [clojure.test :refer [deftest is testing]]
            [jepsen.echo.checker :refer [analyze]]))

; History :time and :wall-invoke share one scale here (milliseconds), so a log
; entry's :ts reads as "logged at that moment".

(def nodes ["echo@n1" "echo@n2"])

(defn- broadcast
  "Invoke and ok ops for broadcast `id` from `node`/`boot`, invoked at `t`."
  [id node boot t]
  [{:type :invoke :f :broadcast :value id :time t}
   {:type :ok :f :broadcast :value id :time (inc t) :node node :boot boot :wall-invoke t}])

(defn- msg [from boot id ts] {:type "msg" :from from :boot boot :id id :ts ts})
(defn- boot [b ts] {:type "boot" :boot b :ts ts})
(defn- expired [from ts] {:type "expired" :from from :ts ts})

(defn- reads
  [n1-log n2-log]
  [{:type :ok :f :read :node "echo@n1" :value n1-log :time 5000}
   {:type :ok :f :read :node "echo@n2" :value n2-log :time 5000}])

; Messages 1 and 2 from n1, invoked at 1000 and 2000.
(def two-from-n1 (concat (broadcast 1 "echo@n1" "a" 1000) (broadcast 2 "echo@n1" "a" 2000)))

(def n1-log-complete [(boot "a" 0) (msg "echo@n1" "a" 1 1001) (msg "echo@n1" "a" 2 2001)])

(defn- check
  ([n2-log] (check n2-log {}))
  ([n2-log opts] (check n1-log-complete n2-log opts))
  ([n1-log n2-log opts] (analyze nodes (vec (concat two-from-n1 (reads n1-log n2-log))) opts)))

(deftest clean-run
  (let [r (check [(boot "b" 0) (msg "echo@n1" "a" 1 1005) (msg "echo@n1" "a" 2 2005)])]
    (is (= true (:valid? r)))
    (is (= 2 (:acked-broadcasts r)))
    (is (= 0 (:lost r) (:gaps r)))))

(deftest lost-message
  (let [r (check [(boot "b" 0) (msg "echo@n1" "a" 1 1005)])]
    (is (= false (:valid? r)))
    (is (= 1 (:lost r)))
    (is (= {:kind :lost :subscriber "echo@n2" :producer "echo@n1" :boot "a" :id 2}
           (first (:lost-sample r))))))

(deftest lost-to-producer-crash
  (testing "messages acked by a boot that later restarted may die with its buffer"
    (let [r (check (conj n1-log-complete (boot "a2" 3000))
                   [(boot "b" 0) (msg "echo@n1" "a" 1 1005)]
                   {})]
      (is (= true (:valid? r)))
      (is (= 1 (:lost-to-crash r))))))

(deftest subscriber-restart
  (testing "a restart after the broadcast excuses losing it"
    (let [r (check [(boot "b" 0) (msg "echo@n1" "a" 1 1005) (boot "b2" 2500)])]
      (is (= true (:valid? r)))
      (is (= 1 (:lost-to-subscriber-crash r)))))
  (testing "a restart before the broadcast does not"
    (let [r (check [(boot "b" 0) (msg "echo@n1" "a" 1 1005) (boot "b2" 1500)])]
      (is (= false (:valid? r)))
      (is (= 1 (:lost r))))))

(deftest expiry-and-loss
  (testing "cursor_expired after the broadcast excuses losing it"
    (let [r (check [(boot "b" 0) (msg "echo@n1" "a" 1 1005) (expired "echo@n1" 2500)]
                   {:expiry-allowed? true})]
      (is (= true (:valid? r)))
      (is (= 1 (:lost-after-expiry r)))))
  (testing "cursor_expired before the broadcast does not"
    (let [r (check [(boot "b" 0) (msg "echo@n1" "a" 1 1005) (expired "echo@n1" 1500)]
                   {:expiry-allowed? true})]
      (is (= false (:valid? r)))
      (is (= 1 (:lost r))))))

(deftest gap
  (testing "message 2 logged before message 1, which was acked before 2 was invoked"
    (let [r (check [(boot "b" 0) (msg "echo@n1" "a" 2 2005) (msg "echo@n1" "a" 1 2006)])]
      (is (= false (:valid? r)))
      (is (= 0 (:lost r)))
      (is (= 1 (:gaps r)))
      (is (= {:id 2 :missing 1} (select-keys (first (:gap-sample r)) [:id :missing]))))))

(deftest concurrent-broadcasts-are-not-gaps
  (testing "two broadcasts in flight together may arrive in either order"
    (let [history (concat [{:type :invoke :f :broadcast :value 1 :time 10}
                           {:type :invoke :f :broadcast :value 2 :time 11}
                           {:type :ok :f :broadcast :value 1 :time 12 :node "echo@n1" :boot "a" :wall-invoke 10}
                           {:type :ok :f :broadcast :value 2 :time 13 :node "echo@n1" :boot "a" :wall-invoke 11}]
                          (reads [(boot "a" 0) (msg "echo@n1" "a" 1 14) (msg "echo@n1" "a" 2 15)]
                                 [(boot "b" 0) (msg "echo@n1" "a" 2 14) (msg "echo@n1" "a" 1 15)]))]
      (is (= 0 (:gaps (analyze nodes (vec history) {})))))))

(deftest gap-and-expiry
  (testing "an expiry between the missing message's invoke and m excuses the gap"
    (let [r (check [(boot "b" 0) (expired "echo@n1" 1500) (msg "echo@n1" "a" 2 2005)]
                   {:expiry-allowed? true})]
      (is (= true (:valid? r)))
      (is (= 1 (:gaps-excused r) (:lost-after-expiry r)))))
  (testing "an expiry logged before the missing message was even invoked does not"
    (let [r (check [(boot "b" 0) (expired "echo@n1" 500) (msg "echo@n1" "a" 2 2005)]
                   {:expiry-allowed? true})]
      (is (= false (:valid? r)))
      (is (= 1 (:gaps r) (:lost r)))))
  (testing "the delivery workload rejects an expiry from a producer that never restarted"
    (let [r (check [(boot "b" 0) (expired "echo@n1" 1500) (msg "echo@n1" "a" 2 2005)]
                   {:expiry-allowed? false})]
      (is (= false (:valid? r)))
      (is (= [{:subscriber "echo@n2" :producer "echo@n1" :count 1}]
             (:unexplained-expired r))))))

(deftest expiry-after-producer-restart-is-explained
  (let [r (check (conj n1-log-complete (boot "a2" 3000))
                 [(boot "b" 0) (msg "echo@n1" "a" 1 1005) (msg "echo@n1" "a" 2 2005)
                  (expired "echo@n1" 3100)]
                 {})]
    (is (= true (:valid? r)))
    (is (empty? (:unexplained-expired r)))))

(deftest duplicates-only
  (let [r (check [(boot "b" 0) (msg "echo@n1" "a" 1 1005) (msg "echo@n1" "a" 1 1006)
                  (msg "echo@n1" "a" 2 2005) (msg "echo@n1" "a" 2 2006)])]
    (is (= true (:valid? r)))
    (is (= {"echo@n1" 0 "echo@n2" 2} (:duplicates r)))))

(deftest missing-read-is-unknown
  (let [r (analyze nodes
                   (vec (concat two-from-n1
                                [{:type :ok :f :read :node "echo@n1" :time 5000
                                  :value n1-log-complete}]))
                   {})]
    (is (= :unknown (:valid? r)))
    (is (= ["echo@n2"] (:missing-reads r)))))
