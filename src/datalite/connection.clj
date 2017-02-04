(ns datalite.connection
  (:require [datalite.bootstrap :as boot]
            [datalite.jdbc :as jdbc])
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

(deftype DataliteConnection [^Connection conn]
  java.lang.AutoCloseable
  (close [this]
    (.close conn)))

(defn connect
  "Connects to the specified database file, or a in-memory
  database if no file is given."
  ([]
   (let [conn (sqlite-connect ":memory:")]
     (jdbc/with-tx conn
       (boot/bootstrap conn))
     (->DataliteConnection conn)))
  ([filename]
   (let [conn (sqlite-connect filename)]
     (jdbc/with-tx conn
       (if (boot/schema-exists? conn)
         (when-not (boot/valid-version? conn)
           (throw (ex-info "Unsupported schema in existing database"
                           {:cause :db.error/unsupported-schema
                            :filename filename})))
         (boot/bootstrap conn)))
     (->DataliteConnection conn))))

(defn close
  "Closes the given connection."
  [^DataliteConnection conn]
  (.close conn))

