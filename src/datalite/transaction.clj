(ns datalite.transaction
  (:require [datalite.database :as db]
            [datalite.connection :as conn]
            [datalite.id :as id]
            [datalite.schema :as schema]
            [datalite.system :as sys]
            [datalite.sql :as sql]
            [datalite.util :as util]
            [clojure.string :as str])
  (:import [java.util Date]))

;;;; Tempids

(def ^:private auto-tempid (atom -1000000))

(defn- next-auto-tempid
  []
  (swap! auto-tempid dec))

(deftype DbId [part t])

(defmethod print-method DbId
  [^DbId id ^java.io.Writer w]
  (.write w (str "#db/id" [(.part id) (.t id)])))

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

;;;; Transaction

(defn- current-t
  "Returns the current t counter values."
  [conn]
  (sql/query-first (conn/sql-con conn) "SELECT t, s FROM head LIMIT 1"))

(defn transaction
  "Creates a transaction map."
  [conn]
  (let [[t system-t] (current-t conn)
        db           (db/db conn t)]
    {:conn conn
     :db db
     :t (inc t)
     :tx-id (id/eid sys/part-tx t)
     :auto-temp-id @auto-tempid         ; To generate local unique temp ids
     :instant (Date.)
     :ids {}
     :tempids #{}
     :ops []}))

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
  "Return the entity-id for map form m."
  [tx m]
  (if-let [e (:db/id m)]
    [tx (dissoc m :db/id) e]
    (let [temp-id (dec (:auto-temp-id tx))]
      [(assoc tx :auto-temp-id temp-id) m temp-id])))

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
     (cond
       ;; Transform maps into add ops.
       (map? form)
       (let [[tx m e] (map-form-entity-id tx form)]
         (reduce-kv (fn [tx a v]
                      (rf tx {:op :add :form form :e e :a a :v v}))
                    tx m))

       ;; Transform non-empty sequence forms into ops.
       (and (sequential? form) (seq form))
       (case (first form)
         :db/add (rf tx (atomic-op :add form))
         :db/retract (rf tx (atomic-op :retract form))
         ; TODO: Resolve/call functions!
         (throw-invalid-form form))

       :else
       (throw-invalid-form form)))))

(defn reverse-attr
  "For 'reverse attributes' that are keywords with names starting
  with an underscore, reverse the association: [:e :ns/_ref :v]
  becomes [:v :ns/ref :e]."
  [rf]
  (fn
    ([tx] (rf tx))
    ([tx {:keys [e a v] :as op}]
     (if (and (keyword? a)
              (str/starts-with? (name a) "_"))
       (rf tx (merge op {:e v :v e
                         :a (keyword (namespace a)
                                     (subs (name a) 1))}))
       (rf tx op)))))

(defn resolve-attr
  "Resolves the :a value of the op to an entity id, and makes sure that
  it actually is a installed schema attribute.  Stores the attribute
  properties from the schema as :attr in op, so that subsequent steps
  can analyze ops based on the schema."
  [rf]
  (fn
    ([tx] (rf tx))
    ([tx op]
     (let [[tx id] (resolve-id tx (:a op))
           attrs   (schema/attrs (db/schema (:db tx)) id)]
       (when-not (schema/attr? attrs)
         (util/throw-error :db.error/not-an-attribute
                           "supplied value is not an attribute"
                           {:val (:a op)}))
       (rf tx (assoc op :a id :attr attrs))))))

(defn resolve-entity
  "Resolves the :e value of the op to an entity id, unless it is a tempid."
  [rf]
  (fn
    ([tx] (rf tx))
    ([tx {:keys [e] :as op}]
     (if (tempid? e)
       (rf (update tx :tempids conj e) op)
       (let [[tx id] (resolve-id tx e)]
         (rf tx (assoc op :e id)))))))

; TODO: Some operations are allowed, in particular adding :db.install/attribute
; to :db.part/db!
(defn protect-system
  "Checks that the operation does not target a system entity."
  [rf]
  (fn
    ([tx] (rf tx))
    ([tx {:keys [e] :as op}]
     (if (some? (schema/attrs schema/system-schema e))
       (util/throw-error :db.error/modify-system-entity
                         "cannot modify system entity"
                         {:val e})
       (rf tx op)))))

(defn resolve-ref
  "If the :attr has value-type 'ref', resolve the :v to an entity id."
  [rf]
  (fn
    ([tx] (rf tx))
    ([tx {:keys [v attr] :as op}]
     (if (schema/ref? attr)
       (let [[tx id] (resolve-id tx v)]
         (rf tx (assoc op :v id)))
       (rf tx op)))))

(defn collect-ops
  ([tx] tx)
  ([tx op]
   (update tx :ops conj op)))

(defn op->datom
  [tx-id op]
  [(:e op) (:a op) (:v op) tx-id (= :add (:op op))])

(defn tx-report
  "Assembles a transaction report."
  [tx]
  {:db-before (:db tx)
   :db-after (db/db (:conn tx))
   :tx-data (mapv (partial op->datom (:tx-id tx)) (:ops tx))
   :tempids (select-keys (:ids tx) (:tempids tx))})

(def process-tx-data
  (comp
    analyze-form
    reverse-attr
    resolve-attr
    resolve-entity
    #_protect-system))

(defn transact
  [conn tx-data]
  (sql/with-tx
    (conn/sql-con conn)
    (let [tx (transduce process-tx-data
                        collect-ops
                        (transaction conn)
                        tx-data)]
      (tx-report tx))))

(comment
  (let [conn   (conn/connect)
        temp   (tempid :db.part/db -1)
        report (transact
                 conn
                 [[:db/add temp :db/ident :test/string-value]
                  [:db/add temp :db/valueType :db.type/string]
                  [:db/add temp :db/cardinality :db.cardinality/one]
                  [:db/add :db.part/db :db.install/attribute temp]])]
    (println (:tempids report))
    (println (:tx-data report)))

  (let [conn   (conn/connect)
        report (transact
                 conn
                 [{:db/ident :test/string-value
                  :db/valueType :db.type/string
                  :db/cardinality :db.cardinality/one
                  :db.install/_attribute :db.part/db}])]
    (println (:tx-data report)))
  )
