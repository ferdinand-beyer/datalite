(ns datalite.database
  (:require [datalite.connection :as conn]
            [datalite.id :as id]
            [datalite.util :refer [s]])
  (:import [datalite.connection DbConnection]
           [java.sql Connection]))

;;;; Database value

(defn- current-t
  [conn]
  (with-open [stmt (.createStatement (conn/sql-conn conn))
              rs (.executeQuery stmt "SELECT t FROM head LIMIT 1")]
    (.next rs)
    (.getLong rs 1)))

(deftype Database [^DbConnection conn basis-t])

(defn db
  "Constructs a database value."
  ([conn]
   (db conn (current-t conn)))
  ([conn basis-t]
   (->Database conn basis-t)))

(defn ^Connection sql-conn
  "Returns the SQL connection from db."
  [^Database db]
  (conn/sql-conn (.conn db)))

(defn- entity?
  [^Database db e]
  (with-open [stmt (.prepareStatement
                     (sql-conn db)
                     (s "SELECT e FROM data "
                        "WHERE e = ? AND ? BETWEEN ta AND tr "
                        "LIMIT 1"))]
    (.setLong stmt 1 e)
    (.setLong stmt 2 (.basis-t db))
    (with-open [rs (.executeQuery stmt)]
      (.next rs))))

;;;; Entity Identifier

(defprotocol Identifier
  (identifier->eid [x db]))

(extend-protocol Identifier
  Long
  (identifier->eid [e db]
    (when (entity? db e) e)))

