(ns datalite.connection
  (:require [datalite.bootstrap :as boot]
            [datalite.sql :as sql]
            [datalite.util :as util])
  (:import [java.sql Connection]
           [org.sqlite SQLiteConfig SQLiteConfig$TransactionMode]))

(defn- ^SQLiteConfig sqlite-config
  []
  (doto (SQLiteConfig.)
    (.setTransactionMode SQLiteConfig$TransactionMode/IMMEDIATE)))

(defn- sqlite-connect
  [filename]
  (.createConnection (sqlite-config)
                     (str "jdbc:sqlite:" filename)))

(deftype DbConnection [^Connection conn]
  java.lang.AutoCloseable
  (close [this]
    (.close conn)))

(defn ^Connection sql-conn
  [^DbConnection conn]
  (.conn conn))

(defn connect
  "Connects to the specified database file, or a in-memory
  database if no file is given."
  ([]
   (let [conn (sqlite-connect ":memory:")]
     (sql/with-tx conn
       (boot/bootstrap conn))
     (->DbConnection conn)))
  ([filename]
   (let [conn (sqlite-connect filename)]
     (sql/with-tx conn
       (if (boot/schema-exists? conn)
         (when-not (boot/valid-version? conn)
           (util/throw-error :db.error/unsupported-schema
                             "Unsupported schema in existing database"
                             {:filename filename}))
         (boot/bootstrap conn)))
     (->DbConnection conn))))

(defn close
  "Closes the given connection."
  [^DbConnection conn]
  (.close conn))

