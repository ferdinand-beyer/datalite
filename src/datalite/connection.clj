(ns datalite.connection
  (:require [datalite.bootstrap :as boot]
            [datalite.sql :as sql]
            [datalite.util :as util])
  (:import [org.sqlite SQLiteConfig SQLiteConfig$TransactionMode]))

(defn- sqlite-config
  ^SQLiteConfig []
  (doto (SQLiteConfig.)
    (.setTransactionMode SQLiteConfig$TransactionMode/IMMEDIATE)))

(defn- sqlite-connect
  [filename]
  (.createConnection (sqlite-config)
                     (str "jdbc:sqlite:" filename)))

(deftype Connection [^java.sql.Connection sql-con]
  java.lang.AutoCloseable
  (close [this]
    (.close sql-con)))

(defn sql-con
  ^java.sql.Connection [^Connection conn]
  (.sql-con conn))

(defn connect
  "Connects to the specified database file, or a in-memory
  database if no file is given."
  ([]
   (let [con (sqlite-connect ":memory:")]
     (sql/with-tx con
       (boot/bootstrap! con))
     (->Connection con)))
  ([filename]
   (let [con (sqlite-connect filename)]
     (sql/with-tx con
       (if (boot/schema-exists? con)
         (when-not (boot/valid-version? con)
           (util/throw-error :db.error/unsupported-schema
                             "Unsupported schema in existing database"
                             {:filename filename}))
         (boot/bootstrap! con)))
     (->Connection con))))

(defn close
  "Closes the given connection."
  [^Connection conn]
  (.close conn))

