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

(defmacro ^:private defattr
  "Define an attribute accessor function that can operate on
  a schema and an attribute map."
  [name doc-string binding & body]
  {:pre [(vector? binding)
         (= 1 (count binding))]}
  `(defn ~name
     {:arglists '([~'schema ~'eid]
                  [~'attrs])
      :doc ~doc-string}
     ([schema# eid#]
      (~name (attrs schema# eid#)))
     (~binding ~@body)))

(defattr ident
  "Returns the ident for an entity id."
  [attrs]
  (get attrs sys/ident))

(defattr value-type
  "Returns the value type for an entity id."
  [attrs]
  (get attrs sys/value-type))

(defattr multival?
  "Returns true if eid identifies a :db.cardinality/many attribute."
  [attrs]
  (= sys/cardinality-many (get attrs sys/cardinality)))

(defattr ref?
  "Returns true if eid identifies an attribute with :db.type/ref
  value type."
  [attrs]
  (= sys/type-ref (get attrs sys/value-type)))

(defattr unique
  "Returns the :db/unique value for an attribute, which may be nil."
  [attrs]
  (get attrs sys/unique))

(defattr has-avet?
  "Returns true if eid identifies an attribute in the AVET index."
  [attrs]
  (boolean (some attrs [sys/index sys/unique])))

(defattr attr?
  "Returns true if the entity identified by eid is
  an attribute."
  [attrs]
  (every? (set (keys attrs)) attr-keys))

(defn attr-info
  "Returns information about the attribute with the given id.
  Supported keys are:

  :id, :ident, :cardinality, :value-type, :unique, :indexed, :has-avet,
  :no-history, :is-component, :fulltext"
  [schema aid]
  (when-let [attrs (attrs schema aid)]
    (when (attr? attrs)
      {:id aid
       :ident (get attrs sys/ident)
       :cardinality (ident schema (get attrs sys/cardinality))
       :value-type (ident schema (get attrs sys/value-type))
       :unique (when-let [u (get attrs sys/unique)]
                 (ident schema u))
       :indexed (get attrs sys/index false)
       :has-avet (has-avet? attrs)
       :no-history (get attrs sys/no-history false)
       :is-component (get attrs sys/is-component false)
       :fulltext (get attrs sys/fulltext false)})))

