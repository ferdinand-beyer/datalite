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

(deftype DbConnection [^Connection sql-con]
  java.lang.AutoCloseable
  (close [this]
    (.close sql-con)))

(defn ^Connection sql-con
  [^DbConnection conn]
  (.sql-con conn))

(defn connect
  "Connects to the specified database file, or a in-memory
  database if no file is given."
  ([]
   (let [con (sqlite-connect ":memory:")]
     (sql/with-tx con
       (boot/bootstrap! con))
     (->DbConnection con)))
  ([filename]
   (let [con (sqlite-connect filename)]
     (sql/with-tx con
       (if (boot/schema-exists? con)
         (when-not (boot/valid-version? con)
           (util/throw-error :db.error/unsupported-schema
                             "Unsupported schema in existing database"
                             {:filename filename}))
         (boot/bootstrap! con)))
     (->DbConnection con))))

(defn close
  "Closes the given connection."
  [^DbConnection conn]
  (.close conn))

