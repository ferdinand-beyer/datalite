(ns datalite.connection
  (:require [datalite.bootstrap :as boot]
            [datalite.jdbc :as jdbc])
  (:import [org.sqlite SQLiteConfig SQLiteConfig$TransactionMode]))

(defn- ^SQLiteConfig sqlite-config
  []
  (doto (SQLiteConfig.)
    (.setTransactionMode SQLiteConfig$TransactionMode/IMMEDIATE)))

(defn- sqlite-connect
  [filename]
  (.createConnection (sqlite-config)
                     (str "jdbc:sqlite:" filename)))

(deftype DataliteConnection [^java.sql.Connection conn]
  java.lang.AutoCloseable
  (close [this]
    (.close conn)))

(defn connect
  ([]
   (let [conn (sqlite-connect ":memory:")]
     (jdbc/with-tx conn
       (boot/bootstrap conn))
     (->DataliteConnection conn)))
  ([filename]
   (let [conn (sqlite-connect filename)]
     (jdbc/with-tx conn
       ; TODO: Check schema!
       (boot/bootstrap conn))
     (->DataliteConnection conn))))

(defn close
  [^DataliteConnection conn]
  (let [c (.conn conn)]
    (.close c)
    (.isClosed c)))

