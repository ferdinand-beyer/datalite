(ns datalite.bootstrap
  "Bootstrap a SQLite database for Datalite."
  (:require [datalite.id :as id]
            [datalite.schema :as schema]
            [datalite.sql :as sql]
            [datalite.system :as sys]
            [datalite.util :refer [s]]
            [datalite.valuetype :as vt]))

(def initial-s 100)
(def initial-t 1000)

(def ^:private schema-version 1)

(def ^:private schema-sql
  [(str "CREATE TABLE meta ("
        " k TEXT NOT NULL PRIMARY KEY,"
        " v NOT NULL"
        ")")
   (str "CREATE TABLE head ("
        " s INTEGER NOT NULL,"
        " t INTEGER NOT NULL"
        ")")
   (str "CREATE TABLE data ("
        " d INTEGER NOT NULL PRIMARY KEY,"
        " e INTEGER NOT NULL,"
        " a INTEGER NOT NULL,"
        " v NOT NULL,"
        " vt INTEGER NOT NULL,"
        " ta INTEGER NOT NULL,"
        " tr INTEGER NOT NULL,"
        " avet INTEGER NOT NULL"
        ")")
    "CREATE INDEX idx_data_eavt ON data(e, a, v, vt, tr, ta)"
    "CREATE INDEX idx_data_aevt ON data(a, e, v, vt, tr, ta)"
    "CREATE INDEX idx_data_avet ON data(a, v, e, vt, tr, ta, avet) WHERE avet = 1"
    (str "CREATE INDEX idx_data_vaet"
         " ON data(v, a, e, vt, tr, ta)"
         " WHERE vt = " sys/type-ref)])

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

(defn populate-meta!
  "Populate the meta table."
  [con]
  (sql/insert! con "meta" {:k :datalite/schema-version
                           :v schema-version}))

(defn populate-head!
  "Populate the head table."
  [con]
  (sql/insert! con "head" {:s initial-s
                           :t initial-t}))

(defn- schema-tuples
  "Returns a sequence of tuples for schema entities."
  [entities]
  (mapcat (fn [[e attrs]]
            (mapcat (fn [[a v]]
                      (if (coll? v)
                        (map (partial vector e a) v)
                        [[e a v]]))
                    attrs))
          entities))

(defn- boot-tx-tuples
  "Returns a sequence of tuples for the bootstrap transaction."
  []
  [[(id/eid sys/part-tx 0) sys/tx-instant (java.util.Date. 0)]])

(defn populate-data!
  "Populate the data table."
  [con]
  (let [schema schema/system-schema
        tuples (concat (schema-tuples (schema/entities schema))
                       (boot-tx-tuples))
        ta 0
        tr id/max-t]
    (sql/insert-many! con "data" [:e :a :v :vt :ta :tr :avet]
                      (map (fn [[e a v]]
                             (let [vt (schema/value-type schema a)
                                   v (vt/coerce-write vt v)
                                   avet (if (schema/has-avet? schema a) 1 0)]
                               [e a v vt ta tr avet]))
                           tuples))))

(defn bootstrap!
  "Bootstrap an empty SQLite database."
  [con]
  (doto con
    (create-schema!)
    (populate-meta!)
    (populate-head!)
    (populate-data!)))

