(ns jepsen.echo.client
  "Talks to node_app over HTTP. Broadcasts carry a unique id; a final read
  fetches each node's delivery log."
  (:require [cheshire.core :as json]
            [clj-http.client :as http]
            [clojure.string :as str]
            [jepsen.client :as client]
            [jepsen.echo.db :as db])
  (:import (java.net ConnectException SocketTimeoutException)))

(defn- url
  [node path]
  (str "http://" node ":" db/port path))

(defn parse-log
  "Turns a JSON-lines delivery log into entry maps with keyword keys."
  [body]
  (->> (str/split-lines body)
       (remove str/blank?)
       (mapv #(json/parse-string % true))))

(defn- broadcast!
  [node op]
  (let [res (http/post (url node (str "/broadcast?id=" (:value op)))
                       {:socket-timeout     5000
                        :connection-timeout 1000
                        :throw-exceptions   false})]
    (if (= 200 (:status res))
      (assoc op :type :ok :boot (:boot (json/parse-string (:body res) true)))
      ; 503 means the producer call timed out; the write may still be buffered.
      (assoc op :type :info :error [:http (:status res) (:body res)]))))

(defn- read-log!
  [node op]
  (let [res (http/get (url node "/log") {:socket-timeout 30000 :connection-timeout 1000})]
    (assoc op :type :ok :value (parse-log (:body res)))))

(defrecord Client [node]
  client/Client
  (open! [this _test node] (assoc this :node node))
  (setup! [_ _test])
  (invoke! [_ _test op]
    ; Wall-clock invoke time, comparable with the ts on the nodes' log entries.
    (let [op (assoc op :node (db/node-name node) :wall-invoke (System/currentTimeMillis))]
      (try
        (case (:f op)
          :broadcast (broadcast! node op)
          :read      (read-log! node op))
        (catch ConnectException e
          (assoc op :type :fail :error [:connect (.getMessage e)]))
        (catch SocketTimeoutException _
          (assoc op :type (if (= :read (:f op)) :fail :info) :error :timeout)))))
  (teardown! [_ _test])
  (close! [_ _test]))

(defn client [] (->Client nil))
