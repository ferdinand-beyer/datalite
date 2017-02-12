(ns datalite.schema
  (:require [clojure.set :refer [map-invert]]
            [datalite.system :as sys]))

(deftype Schema [entities ids])

(defn schema
  "Creates a Schema object for an entity-attribute map."
  [entities]
  (let [ids (into {} (map (fn [[id attrs]]
                            [(get attrs sys/ident) id])
                          entities))]
    (->Schema entities ids)))

(defn entities
  "Returns a map of entities and their attributes."
  [^Schema schema]
  (.entities schema))

(defn id
  "Return the entity id for an ident."
  [^Schema schema ident]
  (get (.ids schema) ident))

(defn attrs
  "Returns a map of attributes for an entity id."
  [^Schema schema eid]
  (get (.entities schema) eid))

(defn attr
  "Returns the value of an entity attribute."
  [schema eid aid]
  (get (attrs schema eid) aid))

(defn ident
  "Returns the ident for an entity id."
  [schema eid]
  (attr schema eid sys/ident))

(defn value-type
  "Returns the value type for an entity id."
  [schema eid]
  (attr schema eid sys/value-type))

(def attr-required
  "Attributes required for attribute entities."
  #{sys/ident sys/value-type sys/cardinality})

(defn attr?
  "Returns true if the entity identified by eid is
  an attribute."
  [schema eid]
  (if-let [attrs (attrs schema eid)]
    (every? (set (keys attrs)) attr-required)
    false))

(defn multival?
  "Returns true if eid identifies a :db.cardinality/many attribute."
  [schema eid]
  (= sys/cardinality-many (attr schema eid sys/cardinality)))

(defn ref?
  "Returns true if eid identifies an attribute with :db.type/ref
  value type."
  [schema eid]
  (= sys/type-ref (attr schema eid sys/value-type)))

(defn has-avet?
  "Returns true if eid identifies an attribute in the AVET index."
  [schema eid]
  (let [attrs (attrs schema eid)]
    (boolean (some attrs [sys/index sys/unique]))))

(def system-schema
  "Schema of system entities."
  (schema sys/entities))

;; TODO: Define on schema!
(defn attr-info
  ([id]
   (when-let [attrs (attrs system-schema id)]
     (attr-info id attrs)))
  ([id attrs]
   (when-let [vt (get attrs sys/value-type)]
     {:id id
      :ident (get attrs sys/ident)
      :cardinality (ident system-schema (get attrs sys/cardinality))
      :value-type (ident system-schema vt)
      :unique (when-let [u (get attrs sys/unique)]
                (ident system-schema u))
      :indexed (get attrs sys/index false)
      :has-avet (boolean (or (get attrs sys/index)
                             (get attrs sys/unique)))
      :no-history (get attrs sys/no-history false)
      :is-component (get attrs sys/is-component false)
      :fulltext (get attrs sys/fulltext false)})))

