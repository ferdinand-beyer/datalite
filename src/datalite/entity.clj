(ns datalite.entity
  "Entity API."
  (:require [datalite.database :as db]))

(declare ^:private ^clojure.lang.IPersistentMap entity-attrs)

(deftype Entity [db eid attrs]
  Object
  (toString [_]
    (str (assoc @attrs :db/id eid)))

  clojure.lang.Associative
  (containsKey [_ k]
    (or (= k :db/id)
        (.containsKey (entity-attrs db eid attrs) k)))
  (entryAt [_ k]
    (if (= k :db/id)
      (clojure.lang.MapEntry/create :db/id eid)
      (.entryAt (entity-attrs db eid attrs) k)))
  (assoc [_ k v]
    (throw (UnsupportedOperationException.)))

  clojure.lang.ILookup
  (valAt [_ k]
    (if (= k :db/id)
      eid
      (.valAt (entity-attrs db eid attrs) k)))
  (valAt [_ k not-found]
    (if (= k :db/id)
      eid
      (.valAt (entity-attrs db eid attrs) k not-found)))

  clojure.lang.IPersistentCollection
  (count [_]
    (.count (entity-attrs db eid attrs)))
  (cons [_ o]
    (throw (UnsupportedOperationException.)))
  (empty [e]
    (throw (UnsupportedOperationException.)))
  (equiv [_ o]
    (and (instance? Entity o)
         (= eid (.eid ^Entity o))))

  clojure.lang.Seqable
  (seq [_]
    (.seq (entity-attrs db eid attrs)))

  db/Identifier
  (-resolve-id [n db] eid))

(defn- make-entity
  "Returns a new, not-realized entity."
  [db eid]
  (->Entity db eid (atom nil)))

(defn- load-entity-attrs
  "Returns an entity's attributes."
  [db eid attrs]
  (swap! attrs merge
         (db/entity-attrs db eid (partial make-entity db))))

(defn- entity-attrs
  "Returns the attributes of an entity, loading and caching
  them as needed."
  [db eid attrs]
  (if (nil? @attrs)
    (load-entity-attrs db eid attrs)
    @attrs))

(defn entity
  "Return an entity by id, ident, or lookup ref."
  [db eid]
  (when-let [e (db/resolve-id db eid)]
    (make-entity db e)))

(defn entity-db
  "Returns the database value that is the basis for this entity."
  [^Entity entity]
  (.db entity))

(defn touch
  [^Entity entity]
  (load-entity-attrs (.db entity) (.eid entity) (.attrs entity))
  entity)

(defmethod print-method Entity
  [^Entity entity ^java.io.Writer w]
  (.write w (.toString entity)))

