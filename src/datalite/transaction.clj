(ns datalite.transaction
  (:require [datalite.database :as db]
            [datalite.connection :as conn]
            [datalite.id :as id]
            [datalite.schema :as schema]
            [datalite.sql :as sql]
            [datalite.util :as util]
            [clojure.string :as str]))

;;;; Tempids

(def ^:private auto-tempid (atom -1000000))

(defn- next-auto-tempid
  []
  (swap! auto-tempid dec))

(deftype DbId [part t])

(defn entity-id
  "Turns db-id into an integer entity id."
  [db ^DbId db-id]
  (let [p (.part db-id)
        t (.t db-id)]
    (if (integer? p)
      (id/eid p t)
      (if-let [pid (db/resolve-id db p)]
        (id/eid pid t)
        (util/throw-error :db.error/invalid-db-id
                          "invalid db/id"
                          {:val db-id})))))

(extend-type DbId db/Identifier
  (-resolve-id [db-id db]
    (db/resolve-id db (entity-id db db-id))))

(defn tempid
  "Generate a tempid in the specified partition. Within the scope
  of a single transaction, tempids map consistently to permanent
  ids. Values of n from -1 to -1000000, inclusive, are reserved for
  user-created tempids."
  ([part]
   (->DbId part (next-auto-tempid)))
  ([part n]
   (->DbId part n)))

(defn tempid?
  [id]
  (or (string? id)
      (and (instance? DbId id)
           (neg? (.t ^DbId id)))
      (and (integer? id)
           (neg? id))))

;;;; Transaction data

(defn transaction
  "Creates an accumulator structure for a transaction."
  [conn]
  (let [db (db/db conn)
        t (db/basis-t db)]
    {:db db
     :conn conn
     :basis-t t
     :t (inc t)
     :tx (id/eid schema/part-tx t)
     :ids {}
     :attrs {}
     :tempids #{}
     :datoms []
     :ops []}))

;;;; Entity id resolution

(defn- resolve-id
  "Resolve id and cache the result in tx."
  [tx id]
  (if-let [id (get-in tx [:ids id])]
    [tx id]
    (if-let [e (db/resolve-id (:db tx) id)]
        [(update tx :ids assoc id e) e]
        (util/throw-error :db.error/not-an-entity
                          "could not resolve entity identifier"
                          {:val id}))))

(defn- attr?
  [am]
  (and (contains? am schema/value-type)
       (contains? am schema/cardinality)))

(defn- resolve-attr
  [tx attr]
  (let [[tx id] (resolve-id tx attr)]
    (if-let [am (get-in tx [:attrs id])]
      [tx id am]
      (let [am (db/attr-map (:db tx) id)]
        (if (attr? am)
          [(update tx :attrs assoc id am) id am]
          (util/throw-error :db.error/not-an-attribute
                            "supplied value is not an attribute"
                            {:val attr}))))))

;;;; Transaction data transducers

(defn invoke-transaction-fns
  [xform]
  (fn
    ([] (xform))
    ([tx] (xform tx))
    ([tx form]
     ; TODO: if list form (sequential?), and first value
     ; is neither :db/add nor :db/retract, look up function,
     ; check, call and recur for every returned form.
     (xform tx form))))

(defn- map-form-e
  "Determine the entity id of a map structure."
  [tx m]
  (if-let [e (:db/id m)]
    [tx (dissoc m :db/id) e]
    [tx m (next-auto-tempid)]))

(defn expand-map-forms
  "Transducer expanding map forms to list forms."
  [xform]
  (fn
    ([tx] (xform tx))
    ([tx form]
     (if (map? form)
       (let [[tx m e] (map-form-e tx form)]
         (reduce-kv (fn [tx a v]
                      (xform tx [:db/add e a v]))
                    tx
                    m))
       (xform tx form)))))

(defn check-base-ops
  "Transducer checking list forms to match a basic
  operation [op e a v] with op being :db/add or
  :db/retract."
  [xform]
  (fn
    ([tx] (xform tx))
    ([tx form]
     (if (and (sequential? form)
              (= 4 (count form))
              (#{:db/add :db/retract} (first form)))
       (xform tx (vec form))
       (util/throw-error :db.error/invalid-tx-op
                         "not a valid transaction operation"
                         {:val form})))))

(defn reverse-refs
  "Transducer resolving reverse attribute references."
  [xform]
  (fn
    ([tx] (xform tx))
    ([tx [op e a v :as form]]
     (if (and (keyword? a)
              (str/starts-with? (name a) "_"))
       (xform tx [op v (keyword (namespace a)
                                (subs (name a) 1)) e])
       (xform tx form)))))

(defn resolve-attributes
  "Transducer resolving attributes."
  [xform]
  (fn
    ([tx] (xform tx))
    ([tx [op e a v :as form]]
     (let [[tx id am] (resolve-attr tx a)]
       (xform tx [op e id v])))))

(defn resolve-entities
  "Transducer resolving entity identifiers in e position."
  [xform]
  (fn
    ([tx] (xform tx))
    ([tx [op e a v :as form]]
     (if (tempid? e)
       (xform (-> tx
                  (update :ids assoc e e)
                  (update :tempids conj e))
              form)
       (let [[tx id] (resolve-id tx e)]
         (xform tx [op id a v]))))))

(defn tx-report
  "Assembles a transaction report from a transaction
  structure."
  ([tx]
   {:db-before (:db tx)
    :db-after (db/db (:conn tx))
    :tx-data (:datoms tx)
    :tempids nil})
  ([tx [op e a v]]
   (update tx :datoms conj [e a v (:tx tx) (= :db/add op)])))

(def process-tx-data
  (comp
    invoke-transaction-fns
    expand-map-forms
    check-base-ops
    reverse-refs
    resolve-attributes
    resolve-entities))

(defn transact
  [conn tx-data]
  (sql/with-tx
    (conn/sql-conn conn)
    (transduce process-tx-data
               tx-report
               (transaction conn)
               tx-data)))

; TODO:
; * Check for temporary ids on the tx partition (used for the tx entity)
; * Determine transaction id
; * Generate forms for a new transaction
; * Resolve attribute entity ids (+ lookup schema)
; * Check/convert value types
; * Resolve ref entity ids
; * Check unique identities
; * Check cardinality
; * Generate retract forms for existing cardinality-one datoms
; * Generate multiple forms if a vector is given for a multi-value
;   attribute
; * Expand nested map forms for ref attributes (generate ids in the parent
;   entity's partition, constrain on component or unique like Datomic?)
; * Resolve entity ids
; * Check any supplied value for :db/txInstant (newer than every existing
;   value, older than current time)
; * Verify all entity ids exist
; * Prevent transactions on system entities
; * Generate missing install datoms for attributes
; * Check required schema entities: ident, valueType, cardinality
; * Prevent unique byte attributes
; * Eliminate redundant datoms (existing, only differ in transaction id)
; * Issue SQL and COMMIT
; * Return a transaction report
;
; Later:
; * Run transaction functions
;
; Done:
; * Normalize to list form
; * Assign implicit temporary entity ids
; * Normalize reverse references (`[e :_a v] -> [v :a e]`)
