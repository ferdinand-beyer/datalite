(ns datalite.transaction-test
  (:require [clojure.test :refer :all]
            [datalite.connection :as conn]
            [datalite.database :as db]
            [datalite.schema :as schema]
            [datalite.system :as sys]
            [datalite.test-util]
            [datalite.transaction :as dt :refer :all]))

(defn collect-tx-data
  ([tx] tx)
  ([tx form]
   (update tx :tx-data conj form)))

(deftest resolve-dbid-test
  (let [conn (conn/connect)
        db (db/db conn)]
    (testing "Non-existing integer partition id"
      (is (nil? (db/resolve-id db (->DbId sys/ident 1)))))
    (is (thrown-info? {:db/error :db.error/invalid-db-id}
                      (db/resolve-id db (->DbId :db.part/foo 1))))
    (is (= sys/ident
           (db/resolve-id db (->DbId sys/part-db sys/ident))))
    (is (= sys/ident
           (db/resolve-id db (->DbId :db.part/db sys/ident))))))

(deftest analyze-atomic-op-test
  (let [rf (@#'dt/analyze-form conj)]
    (let [form [:db/add 1 2 3]]
      (is (= [{:op :add :form form :e 1 :a 2 :v 3}]
             (rf [] form))))
    (let [form [:db/retract 33 32 31]]
      (is (= [{:op :retract :form form :e 33 :a 32 :v 31}]
             (rf [] form))))
    (let [form (list :db/add 1 2 3)]
      (is (= [{:op :add :form form :e 1 :a 2 :v 3}]
             (rf [] form))))
    (let [form (list :db/retract 33 32 31)]
      (is (= [{:op :retract :form form :e 33 :a 32 :v 31}]
             (rf [] form))))
    (is (thrown-info? {:db/error :db.error/invalid-tx-form}
                      (rf [] [:db/unknown 1 2 3])))
    (is (thrown-info? {:db/error :db.error/invalid-tx-form}
                      (rf [] [:db/add])))
    (is (thrown-info? {:db/error :db.error/invalid-tx-form}
                      (rf [] [:db/add 1 2 3 4])))
    (is (thrown-info? {:db/error :db.error/invalid-tx-form}
                      (rf [] [:db/retract 1 2])))
    (is (thrown-info? {:db/error :db.error/invalid-tx-form}
                      (rf [] [:db/retract 1 2 3 4])))
    (is (thrown-info? {:db/error :db.error/invalid-tx-form}
                      (rf [] [])))
    (is (thrown-info? {:db/error :db.error/invalid-tx-form}
                      (rf [] true)))
    (is (thrown-info? {:db/error :db.error/invalid-tx-form}
                      (rf [] :db/add)))
    (is (thrown-info? {:db/error :db.error/invalid-tx-form}
                      (rf [] #{:db/add})))))

(deftest analyze-map-forms-test
  (let [rf (@#'dt/analyze-form conj)]
    (let [form {:db/id 42, :a 1, :b 2}]
      (is (= #{{:op :add :form form :e 42 :a :a :v 1}
               {:op :add :form form :e 42 :a :b :v 2}}
             (rf #{} form))))
    (let [form {:a 1, :b 2}]
      (is (= #{{:op :add :form form :a :a :v 1}
               {:op :add :form form :a :b :v 2}}
             (into #{} (map #(dissoc % :e) (rf [] form))))))))

(deftest reverse-attr-test
  (let [rf (@#'dt/reverse-attr conj)]
    (is (= [:result] (rf [:result])))
    (is (= [{:e 1 :a :x/y :v 2}]
           (rf [] {:e 1 :a :x/y :v 2})))
    (is (= [{:e 1 :a :x/y :v 2}]
           (rf [] {:e 2 :a :x/_y :v 1})))
    (is (= [{:e 1 :a :x/_y :v 2}]
           (rf [] {:e 2 :a :x/__y :v 1})))))

(defn sys-attr
  [aid]
  (schema/attrs schema/system-schema aid))

(deftest resolve-attr-test
  (let [conn (conn/connect)
        tx (@#'dt/transaction conn)
        rf (@#'dt/resolve-attr collect-tx-data)]
    (testing ":aid is stored in op"
      (is (= [{:a :db/ident
               :aid sys/ident
               :attr (sys-attr sys/ident)}]
             (:tx-data (rf tx {:a :db/ident})))))
    (testing "non-attribute entities are not accepted"
      (is (thrown-info?
            {:db/error :db.error/not-an-attribute
             :val :db.type/ref}
            (rf tx {:a :db.type/ref}))))
    (testing "tempids are not accepted"
      (is (thrown-info?
            {:db/error :db.error/not-an-entity
             :val -1}
            (rf tx {:a -1})))
      (is (thrown? IllegalArgumentException
                   (rf tx [:db/add 1 "attr" 3]))))
    (testing "resolution failure raises exception"
      (is (thrown-info?
            {:db/error :db.error/not-an-entity
             :val :foo/bar}
            (rf tx {:a :foo/bar}))))))

(deftest resolve-entities-test
  (let [conn (conn/connect)
        tx (@#'dt/transaction conn)
        rf (@#'dt/resolve-entities collect-tx-data)]
    (testing "e-value is replaced with resolved id"
      (is (= [[:db/add sys/ident 2 3]]
             (:tx-data (rf tx [:db/add :db/ident 2 3])))))
    (testing "resolved identifier is cached in tx"
      (is (= {:db/ident sys/ident}
             (:ids (rf tx [:db/add :db/ident 2 3])))))
    (testing "cache is used for resolution"
      (is (= [[:db/add 42 2 3]]
             (:tx-data (rf (update tx :ids assoc :foo 42)
                           [:db/add :foo 2 3])))))
    (testing "tempids are collected but left alone"
      (is (= [[:db/add -1 2 3]]
             (:tx-data (rf tx [:db/add -1 2 3]))))
      (is (= #{-1}
             (:tempids (rf tx [:db/add -1 2 3])))))
    (testing "resolution failure raises exception"
      (is (thrown-info? {:db/error :db.error/not-an-entity
                         :val :foo/bar}
                        (rf tx [:db/add :foo/bar 2 3]))))))

