(ns datalite.bootstrap
  "Bootstrap a Datalite datbase."
  (:require [datalite.schema :as schema])
  (:import [java.sql Connection]))

(def initial-s 100)
(def initial-t 1000)

(def ^:private schema-version 1)

(def ^:private schema-sql
  ["CREATE TABLE meta (
      k TEXT NOT NULL PRIMARY KEY,
      v NOT NULL
    )"

    "CREATE TABLE seq (
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
  (let [sql "SELECT COUNT(*) FROM sqlite_master WHERE type = ? AND name = ?"]
    (with-open [stmt (.prepareStatement conn sql)]
      (.setString stmt 1 type)
      (.setString stmt 2 name)
      (with-open [rs (.executeQuery stmt)]
        (.next rs)
        (pos? (.getInt rs 1))))))

(defn- table-exists?
  "Returns true if a table with name table-name exists in conn."
  [conn table-name]
  (sqlite-exists? conn "table" table-name))

(defn schema-exists?
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
  [^Connection conn meta-key]
  (with-open [stmt (.prepareStatement conn "SELECT v FROM meta WHERE k = ?")]
    (.setString stmt 1 (str meta-key))
    (with-open [rs (.executeQuery stmt)]
      (when (.next rs)
        (.getObject rs 1)))))

(defn valid-version?
  [conn]
  (= schema-version (get-meta conn :datalite/schema-version)))

(defn valid-schema?
  [conn]
  (and (schema-exists? conn)
       (valid-version? conn)))

(defn bootstrap-meta
  [^Connection conn]
  (with-open [stmt (.prepareStatement conn "INSERT INTO meta (k, v) VALUES (?, ?)")]
    (.setString stmt 1 (str :datalite/schema-version))
    (.setInt stmt 2 schema-version)
    (.executeUpdate stmt)))

(defn bootstrap-seq
  [^Connection conn]
  (with-open [stmt (.prepareStatement conn "INSERT INTO seq (s, t) VALUES (?, ?)")]
    (.setInt stmt 1 initial-s)
    (.setInt stmt 2 initial-t)
    (.executeUpdate stmt)))

(defn bootstrap
  [conn]
  (doto conn
    (create-schema)
    (bootstrap-meta)
    (bootstrap-seq)))

