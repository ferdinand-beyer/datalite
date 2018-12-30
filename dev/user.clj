(ns user
  (:require [clojure.java.javadoc :refer [javadoc]]
            [clojure.pprint :refer [pprint]]
            [clojure.reflect :refer [reflect]]
            [clojure.repl :refer [apropos dir doc find-doc pst source]]
            [clojure.test :refer [run-all-tests]]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.tools.namespace.repl :refer [refresh refresh-all]]
            [datalite.api :as d]))

(def conn nil)
(def db nil)

(defn refresh-db
  []
  (alter-var-root #'db (fn [db] (d/db conn))))

(defn start
  []
  (alter-var-root #'conn
                  (constantly (d/connect)))
  (refresh-db)
  :ok)

(defn stop
  []
  (alter-var-root #'conn
                  (fn [conn] (when conn (d/close conn))))
  (alter-var-root #'db (constantly nil))
  :ok)

(defn reset
  []
  (stop)
  (refresh :after 'user/start))

(defn test-all []
  (run-all-tests #"datalite\..*"))
