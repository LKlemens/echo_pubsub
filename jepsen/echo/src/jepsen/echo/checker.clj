(ns jepsen.echo.checker
  "Checks echo_pubsub's delivery promise against the broadcast history and the
  delivery logs read from every node at the end of the run.

  A stream is one producer incarnation: a node plus the boot id of the VM that
  acked the broadcast. A restart starts a new stream with an empty buffer.

  - Lost: an acked broadcast that a subscriber never logged. Excused, and
    counted separately, when the producer later crashed (its buffer is memory
    only), when the subscriber restarted after the broadcast was invoked (its
    mailbox died with it), or when the subscriber logged cursor_expired for the
    producer after the broadcast was invoked.
  - Gap: a subscriber logged message m but had not yet logged a message x from
    the same stream that was acked before m was invoked. Excused when, before
    m in the log and after x was invoked, the subscriber logged a
    cursor_expired for the producer or restarted.
  - Duplicates are counted and allowed.
  - Unexplained expiry: cursor_expired for a producer that never restarted.
    It fails the run unless the workload expects expiries.

  Time checks compare the client's wall clock at invoke (`:wall-invoke`) with
  the `ts` on log entries; the Docker nodes share the host's clock. A restart
  or expiry that came before a broadcast cannot have lost it."
  (:require [jepsen.checker :as checker]
            [jepsen.echo.db :as db]))

(def ^:private never Long/MAX_VALUE)

(def clock-slack-ms
  "Tolerance when comparing the client's clock with the nodes' clocks."
  100)

(defn- final-logs
  "Node name -> entries of its last successful read."
  [history]
  (reduce (fn [logs op]
            (if (and (= :read (:f op)) (= :ok (:type op)))
              (assoc logs (:node op) (vec (:value op)))
              logs))
          {}
          history))

(defn- summarize
  "Indexes one subscriber's log."
  [entries]
  (reduce-kv
    (fn [s i e]
      (case (:type e)
        "boot"    (update s :boots conj {:idx i :ts (:ts e)})
        "expired" (update-in s [:expired (:from e)] (fnil conj []) {:idx i :ts (:ts e)})
        "msg"     (-> s
                      (update :msgs inc)
                      (update-in [:recv [(:from e) (:boot e)] (:id e)] #(or % i)))
        s))
    {:boots [] :expired {} :recv {} :msgs 0}
    entries))

(defn- resets
  "Entries after which the subscriber may miss messages from `producer`: its
  cursor_expired notices for that producer, and its own restarts (every boot
  entry after the first)."
  [summary producer]
  (concat (get-in summary [:expired producer]) (rest (:boots summary))))

(defn- reset-after?
  "Whether any of `entries` was logged after wall time `t`, and before log
  position `pos` when given."
  ([entries t] (reset-after? entries t never))
  ([entries t pos]
   (boolean (some #(and (< (:idx %) pos) (> (:ts %) (- t clock-slack-ms))) entries))))

(defn- count-below
  "How many of the sorted longs in `ts` are below `t`."
  [ts t]
  (loop [lo 0 hi (count ts)]
    (if (< lo hi)
      (let [mid (quot (+ lo hi) 2)]
        (if (< (nth ts mid) t) (recur (inc mid) hi) (recur lo mid)))
      lo)))

(defn- stream-gaps
  "Gaps in one stream at one subscriber. `stream` is sorted by completion time."
  [stream invoked-at summary subscriber]
  (let [[producer boot] ((juxt :node :boot) (first stream))
        recv     (get-in summary [:recv [producer boot]] {})
        pos      (mapv #(get recv (:value %) never) stream)
        ts       (mapv :time stream)
        ; prefmax[k] = latest log position among the first k acked messages
        prefmax  (vec (reductions max -1 pos))]
    (for [[m p] (map vector stream pos)
          :when (not= p never)
          :let [k (count-below ts (invoked-at (:value m)))]
          :when (> (nth prefmax k) p)
          :let [missing (->> (map vector (subvec stream 0 k) (subvec pos 0 k))
                             (filter (fn [[_ px]] (> px p)))
                             (map first))
                latest  (apply max (map :wall-invoke missing))]]
      {:subscriber subscriber
       :producer   producer
       :boot       boot
       :id         (:value m)
       :missing    (count missing)
       ; one reset after the latest-invoked missing message covers them all
       :excused?   (reset-after? (resets summary producer) latest p)})))

(defn- loss-kind
  [summary final-boots op]
  (let [{:keys [node boot value wall-invoke]} op]
    (cond
      (get-in summary [:recv [node boot] value])                     nil
      (not= boot (final-boots node))                                 :lost-to-crash
      (reset-after? (rest (:boots summary)) wall-invoke)             :lost-to-subscriber-crash
      (reset-after? (get-in summary [:expired node]) wall-invoke)    :lost-after-expiry
      :else                                                          :lost)))

(defn analyze
  "Pure analysis behind the checker; see the namespace doc for the rules."
  [nodes history {:keys [expiry-allowed?]}]
  (let [logs        (final-logs history)
        missing     (remove logs nodes)
        summaries   (into {} (for [[n entries] logs] [n (summarize entries)]))
        final-boots (into {} (for [[n entries] logs]
                               [n (:boot (last (filter #(= "boot" (:type %)) entries)))]))
        boot-counts (into {} (for [[n s] summaries] [n (count (:boots s))]))
        invoked-at  (into {} (for [op history
                                   :when (and (= :broadcast (:f op)) (= :invoke (:type op)))]
                               [(:value op) (:time op)]))
        acked       (filter #(and (= :broadcast (:f %)) (= :ok (:type %))) history)
        streams     (->> acked (group-by (juxt :node :boot)) vals (map #(vec (sort-by :time %))))
        losses      (for [[s summary] summaries
                          op acked
                          :let [kind (loss-kind summary final-boots op)]
                          :when kind]
                      {:kind kind :subscriber s :producer (:node op) :boot (:boot op) :id (:value op)})
        by-kind     (group-by :kind losses)
        gaps        (for [[s summary] summaries
                          stream streams
                          gap (stream-gaps stream invoked-at summary s)]
                      gap)
        {bad-gaps false excused-gaps true} (group-by :excused? gaps)
        unexplained (for [[s summary] summaries
                          [producer idxs] (:expired summary)
                          :when (= 1 (boot-counts producer))]
                      {:subscriber s :producer producer :count (count idxs)})
        failed?     (or (seq (:lost by-kind))
                        (seq bad-gaps)
                        (and (not expiry-allowed?) (seq unexplained)))]
    {:valid?                   (cond failed?        false
                                     (seq missing) :unknown
                                     :else          true)
     :acked-broadcasts         (count acked)
     :missing-reads            (vec missing)
     :lost                     (count (:lost by-kind))
     :lost-sample              (vec (take 10 (:lost by-kind)))
     :lost-to-crash            (count (:lost-to-crash by-kind))
     :lost-to-subscriber-crash (count (:lost-to-subscriber-crash by-kind))
     :lost-after-expiry        (count (:lost-after-expiry by-kind))
     :gaps                     (count bad-gaps)
     :gap-sample               (vec (take 10 bad-gaps))
     :gaps-excused             (count excused-gaps)
     :duplicates               (into (sorted-map)
                                     (for [[n s] summaries]
                                       [n (- (:msgs s) (reduce + (map count (vals (:recv s)))))]))
     :expired                  (into (sorted-map)
                                     (for [[n s] summaries]
                                       [n (reduce + (map count (vals (:expired s))))]))
     :unexplained-expired      (vec unexplained)}))

(defn delivery
  "Checker for at-least-once delivery. Options: `:expiry-allowed?`."
  [opts]
  (reify checker/Checker
    (check [_ test history _opts]
      (analyze (map db/node-name (:nodes test)) history opts))))
