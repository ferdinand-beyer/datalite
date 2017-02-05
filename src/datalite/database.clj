(ns datalite.database
  (:require [datalite.connection :as conn]
            [datalite.id :as id]
            [datalite.schema :as schema]
            [datalite.util :as util :refer [s]])
  (:import [datalite.connection DbConnection]
           [java.sql Connection]))

;;;; Database value

(defn- current-t
  [conn]
  (with-open [stmt (.createStatement (conn/sql-conn conn))
              rs (.executeQuery stmt "SELECT t FROM head LIMIT 1")]
    (.next rs)
    (.getLong rs 1)))

(deftype Database [^DbConnection conn basis-t])

(defn db
  "Constructs a database value."
  ([conn]
   (db conn (current-t conn)))
  ([conn basis-t]
   (->Database conn basis-t)))

(defn ^Connection sql-conn
  "Returns the SQL connection from db."
  [^Database db]
  (conn/sql-conn (.conn db)))

;;;; Entity queries

(defn- entity?
  "Returns true if the entity id e exists."
  [^Database db e]
  (with-open [stmt (.prepareStatement
                     (sql-conn db)
                     (s "SELECT e FROM data "
                        "WHERE e = ? AND ? BETWEEN ta AND tr "
                        "LIMIT 1"))]
    (.setLong stmt 1 e)
    (.setLong stmt 2 (.basis-t db))
    (with-open [rs (.executeQuery stmt)]
      (.next rs))))

; TODO: Protocols to get/set v with value-type hint

(defn- ident->eid
  [^Database db kwd]
  (with-open [stmt (.prepareStatement
                     (sql-conn db)
                     (s "SELECT e FROM data "
                        "WHERE a = ? AND v = ? "
                        "AND ? BETWEEN ta AND tr "
                        "AND avet = 1 "
                        "LIMIT 1"))]
    (.setLong stmt 1 schema/ident)
    (.setString stmt 2 (str kwd))
    (.setLong stmt 3 (.basis-t db))
    (with-open [rs (.executeQuery stmt)]
      (when (.next rs)
        (.getLong rs 1)))))

(defn- unique->eid
  [^Database db a v]
  (with-open [stmt (.prepareStatement
                     (sql-conn db)
                     (s "SELECT e FROM data "
                        "WHERE a = ? AND v = ? "
                        "AND ? BETWEEN ta AND tr "
                        "AND avet = 1 "
                        "LIMIT 1"))]
    (.setLong stmt 1 a)
    (.setObject stmt 2 v)
    (.setLong stmt 3 (.basis-t db))
    (with-open [rs (.executeQuery stmt)]
      (when (.next rs)
        (.getLong rs 1)))))

(defn attr-map
  [^Database db e]
  (with-open [stmt (.prepareStatement
                     (sql-conn db)
                     (s "SELECT a, v FROM data "
                        "WHERE e = ? AND ? BETWEEN ta AND tr"))]
    (.setLong stmt 1 e)
    (.setLong stmt 2 (.basis-t db))
    (with-open [rs (.executeQuery stmt)]
      (loop [m nil]
        (if (.next rs)
          (recur
            (let [a (.getLong rs 1)
                  v (.getObject rs 2)]
              (assoc m a
                     (if-let [old-v (get m a)]
                       (if (set? old-v)
                         (conj old-v v)
                         (hash-set old-v v))
                       v))))
          m)))))

;;;; Schema queries

; TODO: Use schema/attr-info for system ids
(defn attr-info
  [db attrid]
  (when-let [attrs (attr-map db attrid)]
    (schema/attr-info attrid attrs)))

;;;; Entity Identifier

(defprotocol Identifier
  (-entity-id [x db]))

(defn entity-id
  "Returns the entity id for an entity identifier."
  [db identifier]
  (-entity-id identifier db))

(extend-protocol Identifier
  Long
  (-entity-id [n db]
    (when (entity? db n) n))

  clojure.lang.Keyword
  (-entity-id [kwd db]
    (ident->eid db kwd))

  clojure.lang.Sequential
  (-entity-id [lookup-ref db]
    (when-not (and (= 2 (count lookup-ref)))
      (util/throw-error :db.error/invalid-lookup-ref
                        "lookup refs must be vectors with exactly 2 elements"
                        {:val lookup-ref}))
    (let [[attr v] lookup-ref
          a (-entity-id attr db)]
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

