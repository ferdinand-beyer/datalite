(ns datalite.database
  (:require [datalite.connection :as conn]
            [datalite.id :as id]
            [datalite.schema :as schema]
            [datalite.sql :as sql]
            [datalite.util :as util :refer [s]]
            [datalite.valuetype :as vt]))

;;;; Database value

(defn- current-t
  [conn]
  (sql/query-val (conn/sql-con conn) "SELECT t FROM head LIMIT 1"))

(deftype Database [^datalite.connection.DbConnection conn
                   basis-t])

(defn db
  "Constructs a database value."
  ([conn]
   (db conn (current-t conn)))
  ([conn basis-t]
   (->Database conn basis-t)))

(defn basis-t
  [^Database db]
  (.basis-t db))

(defn sql-con
  "Returns the SQL connection from db."
  ^java.sql.Connection [^Database db]
  (conn/sql-con (.conn db)))

;;;; Entity queries

(defn- entity?
  "Returns true if the entity id e exists."
  [^Database db e]
  (some? (sql/query-val (sql-con db)
                        (s "SELECT e FROM data "
                           "WHERE e = ? AND ? BETWEEN ta AND tr "
                           "LIMIT 1")
                        [(long e) (basis-t db)])))

; TODO: Protocols to get/set v with value-type hint

(defn- ident->eid
  [^Database db kwd]
  (sql/query-val (sql-con db)
                 (s "SELECT e FROM data "
                    "WHERE a = ? AND v = ? "
                    "AND ? BETWEEN ta AND tr "
                    "AND avet = 1 "
                    "LIMIT 1")
                 [schema/ident (str kwd) (basis-t db)]))

(defn- unique->eid
  [^Database db a v]
  (sql/query-val (sql-con db)
                 (s "SELECT e FROM data "
                        "WHERE a = ? AND v = ? "
                        "AND ? BETWEEN ta AND tr "
                        "AND avet = 1 "
                        "LIMIT 1")
                 [(long a) v (basis-t db)]))

(defn- assoc-multi
  [m k v]
  (update m k (fn [old-v]
                (if old-v
                  (if (set? old-v)
                    (conj old-v v)
                    #{old-v v})
                  v))))

(defn attr-map
  [^Database db e]
  (sql/run-query (sql-con db)
                 (s "SELECT d.a, d.v, s.v FROM data d"
                    " INNER JOIN data s ON d.a = s.e"
                    " WHERE d.e = ? AND s.a = ?"
                    " AND ? BETWEEN d.ta AND d.tr"
                    " AND ? BETWEEN s.ta AND s.tr")
                 [(long e) schema/value-type (basis-t db) (basis-t db)]
                 #(reduce (fn [m [a v vt]]
                            (assoc-multi m a (vt/coerce-read vt v)))
                          nil %)))

;;;; Schema queries

; TODO: Use schema/attr-info for system ids
(defn attr-info
  [db attrid]
  (when-let [attrs (attr-map db attrid)]
    (schema/attr-info attrid attrs)))

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
    (ident->eid db kwd))

  clojure.lang.Sequential
  (-resolve-id [lookup-ref db]
    (when-not (and (= 2 (count lookup-ref)))
      (util/throw-error :db.error/invalid-lookup-ref
                        "lookup refs must be vectors with exactly 2 elements"
                        {:val lookup-ref}))
    (let [[attr v] lookup-ref
          a (-resolve-id attr db)]
      (when-not a
        (util/throw-error :db.error/invalid-lookup-ref
                          "unknown attribute"
                          {:val attr}))
      (let [am (attr-map db a)]
        (when-not (get am schema/unique)
          (util/throw-error :db.error/invalid-lookup-ref
                            "lookup ref attribute is not :db/unique"
                            {:val attr}))
        (unique->eid db a v)))))

