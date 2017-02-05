(ns datalite.bootstrap
  "Bootstrap a Datalite datbase."
  (:require [datalite.id :as id]
            [datalite.schema :as schema]
            [datalite.util :refer [s]])
  (:import [java.sql Connection]))

(def initial-s 100)
(def initial-t 1000)

(def ^:private schema-version 1)

(def ^:private schema-sql
  ["CREATE TABLE meta (
      k TEXT NOT NULL PRIMARY KEY,
      v NOT NULL
    )"

    "CREATE TABLE head (
      s INTEGER NOT NULL,
      t INTEGER NOT NULL
    )"

    "CREATE TABLE data (
      d INTEGER NOT NULL PRIMARY KEY,
      e INTEGER NOT NULL,
      a INTEGER NOT NULL,
      v NOT NULL,
      ta INTEGER NOT NULL,
      tr INTEGER NOT NULL,
      avet INTEGER NOT NULL,
      vaet INTEGER NOT NULL
    )"

    "CREATE INDEX idx_data_eavt ON data(e, a, v, tr, ta)"
    "CREATE INDEX idx_data_aevt ON data(a, e, v, tr, ta)"
    "CREATE INDEX idx_data_avet ON data(a, v, e, tr, ta, avet) WHERE avet = 1"
    "CREATE INDEX idx_data_vaet ON data(v, a, e, tr, ta, vaet) WHERE vaet = 1"])

(defn- sqlite-exists?
  [^Connection conn type name]
  (with-open [stmt (.prepareStatement
                       conn
                       (s "SELECT COUNT(*) FROM sqlite_master"
                          " WHERE type = ? AND name = ?"))]
      (.setString stmt 1 type)
      (.setString stmt 2 name)
      (with-open [rs (.executeQuery stmt)]
        (.next rs)
        (pos? (.getLong rs 1)))))

(defn- table-exists?
  "Returns true if a table with name table-name exists in conn."
  [conn table-name]
  (sqlite-exists? conn "table" table-name))

(defn schema-exists?
  "Returns true if it looks like a Datalite schema exists in
  the SQLite database."
  [conn]
  (table-exists? conn "meta"))

(defn create-schema
  "Create the SQLite database schema in conn."
  [^Connection conn]
  (with-open [stmt (.createStatement conn)]
    (doseq [sql schema-sql]
      (.addBatch stmt sql))
    (.executeBatch stmt)))

(defn- get-meta
  "Returns a value from the meta database table for the
  given meta-key."
  [^Connection conn meta-key]
  (with-open [stmt (.prepareStatement
                     conn
                     "SELECT v FROM meta WHERE k = ?")]
    (.setString stmt 1 (str meta-key))
    (with-open [rs (.executeQuery stmt)]
      (when (.next rs)
        (.getObject rs 1)))))

(defn valid-version?
  "Returns true if the schema version in the SQLite database
  is valid."
  [conn]
  (= schema-version (get-meta conn :datalite/schema-version)))

(defn valid-schema?
  [conn]
  (and (schema-exists? conn)
       (valid-version? conn)))

(defn bootstrap-meta
  "Bootstrap meta data."
  [^Connection conn]
  (with-open [stmt (.prepareStatement
                     conn
                     "INSERT INTO meta (k, v) VALUES (?, ?)")]
    (.setString stmt 1 (str :datalite/schema-version))
    (.setLong stmt 2 schema-version)
    (.executeUpdate stmt)))

(defn bootstrap-head
  "Bootstrap initial head values."
  [^Connection conn]
  (with-open [stmt (.prepareStatement
                     conn
                     "INSERT INTO head (s, t) VALUES (?, ?)")]
    (.setLong stmt 1 initial-s)
    (.setLong stmt 2 initial-t)
    (.executeUpdate stmt)))

(defn avet?
  "Return true if a datom for a shall be added to the
  AVET index."
  [a]
  (let [attr (get schema/system-attributes a)]
    (boolean
      (or (get attr schema/index)
          (get attr schema/unique)))))

(defn vaet?
  "Return true if a datom for a shall be added to the
  VAET index."
  [a]
  (= schema/type-ref
     (get-in schema/system-attributes [a schema/value-type])))

(defn system-datoms
  "Returns a sequence of system datoms."
  []
  (mapcat (fn [[e attrs]]
            (map (fn [[a v]]
                   [e a v])
                 attrs))
          schema/system-attributes))

(defn boot-tx-datoms
  "Returns a sequence of datoms for the bootstrap transaction."
  []
  [[(id/eid schema/part-tx 0) schema/tx-instant 0]])

(defn bootstrap-data
  "Insert boot datoms into the data table."
  [^Connection conn]
  (let [datoms (concat (system-datoms) (boot-tx-datoms))
        ta 0
        tr id/max-t]
    (with-open [stmt (.prepareStatement
                       conn
                       (s "INSERT INTO data"
                          " (e, a, v, ta, tr, avet, vaet)"
                          " VALUES (?, ?, ?, ?, ?, ?, ?)"))]
      (doseq [[e a v] datoms]
        (doto stmt
          (.setLong 1 e)
          (.setLong 2 a)
          (.setObject 3 v)
          (.setLong 4 ta)
          (.setLong 5 tr)
          (.setBoolean 6 (avet? a))
          (.setBoolean 7 (vaet? a))
          (.executeUpdate))))))

(defn bootstrap
  "Bootstrap an empty SQLite database."
  [conn]
  (doto conn
    (create-schema)
    (bootstrap-meta)
    (bootstrap-head)
    (bootstrap-data)))

