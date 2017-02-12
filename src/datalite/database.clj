(ns datalite.database
  (:require [datalite.connection :as conn]
            [datalite.id :as id]
            [datalite.schema :as schema]
            [datalite.sql :as sql]
            [datalite.system :as sys]
            [datalite.util :as util :refer [s]]
            [datalite.valuetype :as vt]))

(defn- read-attr
  "Returns a key-value pair for a database tuple."
  [attrs [a v vt]]
  (let [v (vt/coerce-read vt v)]
    [a (if-let [old-v (get attrs a)]
         (if (coll? old-v)
           (conj old-v v)
           #{old-v v})
         v)]))

(defn- map-attrs
  "Returns a transducer that calls (f result input)."
  [f]
  (fn [rf]
    (fn
      ([] (rf))
      ([attrs] (rf attrs))
      ([attrs tuple]
       (rf attrs (f attrs tuple))))))

(def ^:private make-attr-maps
  "Returns a transducer transforming [e a v vt] tuples into pairs
  [e attrs] according to schema."
  (comp (partition-by first)
        (map (fn [tuples]
               [(ffirst tuples)
                (into {}
                      (comp (map #(subvec % 1))
                            (map-attrs read-attr))
                      tuples)]))))

;;;; Database value

(defn- current-t
  [conn]
  (sql/query-val (conn/sql-con conn) "SELECT t FROM head LIMIT 1"))

(defn- fetch-schema-entities
  "Fetch a schema entities map from the database."
  [conn t]
  (sql/run-query
    (conn/sql-con conn)
    (s "SELECT e, a, v, vt FROM data"
       " WHERE e <= ?"
       " AND ? BETWEEN ta AND tr"
       " ORDER BY e, a")
    [id/max-t t]
    (partial into {} make-attr-maps)))

(deftype Database [^datalite.connection.Connection conn
                   basis-t
                   schema])

(defn db
  "Constructs a database value."
  ([conn]
   (db conn (current-t conn)))
  ([conn basis-t]
   (->Database conn basis-t
               (schema/schema (fetch-schema-entities conn basis-t)))))

(defn basis-t
  [^Database db]
  (.basis-t db))

(defn sql-con
  "Returns the SQL connection from db."
  ^java.sql.Connection [^Database db]
  (conn/sql-con (.conn db)))

(defn schema
  "Returns the Schema as seen by db."
  [^Database db]
  (.schema db))

;;;; Entity queries

(defn- entity?
  "Returns true if the entity id e exists."
  [db e]
  (some? (sql/query-val (sql-con db)
                        (s "SELECT e FROM data "
                           "WHERE e = ? AND ? BETWEEN ta AND tr "
                           "LIMIT 1")
                        [(long e) (basis-t db)])))

(defn- unique-lookup
  "Looks up an entity by its value of an unique attribute."
  [db a v]
  (let [schema (schema db)
        vt (schema/value-type schema a)]
    (sql/query-val
      (sql-con db)
      (s "SELECT e FROM data "
         "WHERE a = ? AND v = ? "
         "AND ? BETWEEN ta AND tr "
         "AND avet = 1 "
         "LIMIT 1")
      [(long a)
       (vt/coerce-write vt v)
       (basis-t db)])))

(defn- schema-attr-reader
  [schema entity]
  (fn [attrs [a v vt]]
    (let [attr-meta (schema/attrs schema a)
          k (schema/ident attr-meta)
          v (vt/coerce-read vt v)
          v (if (schema/ref? attr-meta)
              (or (schema/ident schema v)
                  (entity v)
                  v)
              v)]
      [(schema/ident attr-meta)
       (if (schema/multival? attr-meta)
         (conj (get attrs k #{}) v)
         v)])))

(defn entity-attrs
  [db eid entity]
  (sql/run-query
    (sql-con db)
    (s "SELECT a, v, vt FROM data"
       " WHERE e = ?"
       " AND ? BETWEEN ta AND tr")
    [(long eid) (basis-t db)]
    (partial into {} (map-attrs (schema-attr-reader (schema db) entity)))))

;;;; Entity Identifier

(defprotocol Identifier
  (-resolve-id [x db]))

(defn resolve-id
  "Returns the entity id for an entity identifier."
  [db identifier]
  (-resolve-id identifier db))

(extend-protocol Identifier
  Long
  (-resolve-id [n db]
    (when (entity? db n) n))

  clojure.lang.Keyword
  (-resolve-id [kwd db]
    (schema/id (schema db) kwd))

  clojure.lang.Sequential
  (-resolve-id [lookup-ref db]
    (when-not (and (= 2 (count lookup-ref)))
      (util/throw-error :db.error/invalid-lookup-ref
                        "lookup refs must be vectors with exactly 2 elements"
                        {:val lookup-ref}))
    (let [[attr v] lookup-ref
          aid (-resolve-id attr db)]
      (when-not aid
        (util/throw-error :db.error/invalid-lookup-ref
                          "unknown attribute"
                          {:val attr}))

      (when-not (schema/unique (schema db) aid)
        (util/throw-error :db.error/invalid-lookup-ref
                          "lookup ref attribute is not :db/unique"
                          {:val attr}))
      (unique-lookup db aid v))))

