(ns datalite.schema
  (:require [clojure.set :refer [map-invert]]
            [datalite.system :as sys]))

(deftype Schema [entities ids])

(defn schema
  [entities]
  (let [ids (into {} (map (fn [[id attrs]]
                            [(get attrs sys/ident) id])
                          entities))]
    (->Schema entities ids)))

(defn id
  [^Schema schema ident]
  (get (.ids schema) ident))

(defn entities
  [^Schema schema]
  (.entities schema))

(defn attrs
  [^Schema schema id]
  (get (.entities schema) id))

(defn attr
  [schema eid aid]
  (get (attrs schema eid) aid))

(defn ident
  [schema eid]
  (attr schema eid sys/ident))

(def attr-required
  "Attributes required for attribute entities."
  #{sys/ident sys/value-type sys/cardinality})

(defn attr?
  [schema id]
  (if-let [attrs (attrs schema id)]
    (every? (set (keys attrs)) attr-required)
    false))

(defn multival?
  [schema id]
  (= sys/cardinality-many (attr schema id sys/cardinality)))

(defn ref?
  [schema id]
  (= sys/type-ref (attr schema id sys/value-type)))

(defn has-avet?
  [schema id]
  (let [attrs (attrs schema id)]
    (boolean (some attrs [sys/index sys/unique]))))

(def system-schema (schema sys/entities))

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

