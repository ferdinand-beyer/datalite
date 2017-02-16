(ns datalite.transaction
  (:require [datalite.database :as db]
            [datalite.connection :as conn]
            [datalite.id :as id]
            [datalite.schema :as schema]
            [datalite.system :as sys]
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
  (cond
    (instance? DbId id) (neg? (.t ^DbId id))
    (integer? id) (neg? id)
    :else (string? id)))

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
     :tx (id/eid sys/part-tx t)
     :ids {}
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

;;;; Transaction data transducers

(defn- throw-invalid-form
  [form]
  (util/throw-error :db.error/invalid-tx-form
                    "not a valid tx-data form"
                    {:val form}))

(defn- map-form-entity-id
  [tx m]
  (if-let [e (:db/id m)]
    [tx (dissoc m :db/id) e]
    [tx m (next-auto-tempid)]))

(defn- atomic-op
  [op form]
  (if (= 4 (count form))
    (let [[_ e a v] form]
      {:op op :form form :e e :a a :v v})
    (throw-invalid-form form)))

(defn analyze-form
  [rf]
  (fn
    ([tx] (rf tx))
    ([tx form]
     (if (map? form)
       (let [[tx m e] (map-form-entity-id tx form)]
         (reduce-kv (fn [tx a v]
                      (rf tx {:op :add :form form :e e :a a :v v}))
                    tx m))
       (if (and (sequential? form) (seq form))
         (case (first form)
           :db/add (rf tx (atomic-op :add form))
           :db/retract (rf tx (atomic-op :retract form))
           (throw-invalid-form form))
         (throw-invalid-form form))))))

(defn reverse-attr
  "Transducer resolving reverse attribute references."
  [rf]
  (fn
    ([tx] (rf tx))
    ([tx {:keys [e a v] :as form}]
     (if (and (keyword? a)
              (str/starts-with? (name a) "_"))
       (rf tx (merge form {:e v :v e
                           :a (keyword (namespace a)
                                       (subs (name a) 1))}))
       (rf tx form)))))

(defn resolve-attributes
  "Transducer resolving attributes."
  [rf]
  (fn
    ([tx] (rf tx))
    ([tx [op e a v :as form]]
     (let [[tx aid] (resolve-id tx a)]
       (when-not (schema/attr? (db/schema (:db tx)) aid)
         (util/throw-error :db.error/not-an-attribute
                           "supplied value is not an attribute"
                           {:val a}))
       (rf tx [op e aid v])))))

(defn resolve-entities
  "Transducer resolving entity identifiers in e position."
  [rf]
  (fn
    ([tx] (rf tx))
    ([tx [op e a v :as form]]
     (if (tempid? e)
       (rf (update tx :tempids conj e) form)
       (let [[tx id] (resolve-id tx e)]
         (rf tx [op id a v]))))))

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
    analyze-form
    reverse-refs
    resolve-attributes
    resolve-entities))

(defn transact
  [conn tx-data]
  (sql/with-tx
    (conn/sql-con conn)
    (transduce process-tx-data
               tx-report
               (transaction conn)
               tx-data)))

