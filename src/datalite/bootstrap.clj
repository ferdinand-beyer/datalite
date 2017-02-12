(ns datalite.bootstrap
  "Bootstrap a SQLite database for Datalite."
  (:require [datalite.id :as id]
            [datalite.schema :as schema]
            [datalite.sql :as sql]
            [datalite.system :as sys]
            [datalite.util :refer [s]]))

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
  [con type name]
  (pos? (sql/query-val con
                       (s "SELECT COUNT(*) FROM sqlite_master"
                          " WHERE type = ? AND name = ?")
                       [type name])))

(defn- table-exists?
  "Returns true if a table with name table-name exists in conn."
  [con table-name]
  (sqlite-exists? con "table" table-name))

(defn schema-exists?
  "Returns true if it looks like a Datalite schema exists in
  the SQLite database."
  [con]
  (table-exists? con "meta"))

(defn create-schema!
  "Create the SQLite database schema in conn."
  [con]
  (sql/exec-many! con schema-sql))

(defn- get-meta
  "Returns a value from the meta database table for the
  given meta-key."
  [con meta-key]
  (sql/query-val con "SELECT v FROM meta WHERE k = ?" [meta-key]))

(defn valid-version?
  "Returns true if the schema version in the SQLite database
  is valid."
  [con]
  (= schema-version (get-meta con :datalite/schema-version)))

(defn valid-schema?
  [con]
  (and (schema-exists? con)
       (valid-version? con)))

(defn bootstrap-meta!
  "Bootstrap meta data."
  [con]
  (sql/insert! con "meta" {:k :datalite/schema-version
                           :v schema-version}))

(defn bootstrap-head!
  "Bootstrap initial head values."
  [con]
  (sql/insert! con "head" {:s initial-s
                           :t initial-t}))

(defn avet?
  "Return true if a triple for a shall be added to the
  AVET index."
  [aid]
  (schema/has-avet? schema/system-schema aid))

(defn vaet?
  "Return true if a triple for a shall be added to the
  VAET index."
  [a]
  (schema/ref? schema/system-schema a))

(defn system-triples
  "Returns a sequence of system triples."
  []
  (mapcat (fn [[e attrs]]
            (map (fn [[a v]]
                   [e a v])
                 attrs))
          (schema/entities schema/system-schema)))

(defn boot-tx-triples
  "Returns a sequence of triples for the bootstrap transaction."
  []
  [[(id/eid sys/part-tx 0) sys/tx-instant 0]])

(defn bootstrap-data!
  "Insert boot triples into the data table."
  [con]
  (let [triples (concat (system-triples) (boot-tx-triples))
        ta 0
        tr id/max-t]
    (sql/insert-many! con "data" [:e :a :v :ta :tr :avet :vaet]
                      (map (fn [[e a v]]
                             [e a v ta tr (avet? a) (vaet? a)])
                           triples))))

(defn bootstrap!
  "Bootstrap an empty SQLite database."
  [con]
  (doto con
    (create-schema!)
    (bootstrap-meta!)
    (bootstrap-head!)
    (bootstrap-data!)))

