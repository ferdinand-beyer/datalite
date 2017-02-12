(ns datalite.schema
  (:require [clojure.set :refer [map-invert]]
            [datalite.system :as sys]))

(def ^:private attr-keys
  "Attributes required for attribute entities."
  #{sys/ident sys/value-type sys/cardinality})

(deftype Schema [entities ids])

(defn schema
  "Creates a Schema object for an entity-attribute map."
  [entities]
  (let [ids (into {} (map (fn [[id attrs]]
                            [(get attrs sys/ident) id])
                          entities))]
    (->Schema entities ids)))

(def system-schema
  "Schema of system entities."
  (schema sys/entities))

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

(defn multival?
  "Returns true if eid identifies a :db.cardinality/many attribute."
  [schema eid]
  (= sys/cardinality-many (attr schema eid sys/cardinality)))

(defn ref?
  "Returns true if eid identifies an attribute with :db.type/ref
  value type."
  [schema eid]
  (= sys/type-ref (attr schema eid sys/value-type)))

(defn unique
  "Returns the :db/unique value for an attribute, which may be nil."
  [schema eid]
  (attr schema eid sys/unique))

(defn has-avet?
  "Returns true if eid identifies an attribute in the AVET index."
  [schema eid]
  (let [attrs (attrs schema eid)]
    (boolean (some attrs [sys/index sys/unique]))))

(defn- all-attr-keys?
  [attrs]
  (every? (set (keys attrs)) attr-keys))

(defn attr?
  "Returns true if the entity identified by eid is
  an attribute."
  [schema eid]
  (if-let [attrs (attrs schema eid)]
    (all-attr-keys? attrs)
    false))

(defn attr-info
  "Returns information about the attribute with the given id.
  Supported keys are:

  :id, :ident, :cardinality, :value-type, :unique, :indexed, :has-avet,
  :no-history, :is-component, :fulltext"
  [schema aid]
  (when-let [attrs (attrs schema aid)]
    (when (all-attr-keys? attrs)
      {:id aid
       :ident (get attrs sys/ident)
       :cardinality (ident schema (get attrs sys/cardinality))
       :value-type (ident schema (get attrs sys/value-type))
       :unique (when-let [u (get attrs sys/unique)]
                 (ident schema u))
       :indexed (get attrs sys/index false)
       :has-avet (boolean (some attrs [sys/index sys/unique]))
       :no-history (get attrs sys/no-history false)
       :is-component (get attrs sys/is-component false)
       :fulltext (get attrs sys/fulltext false)})))

