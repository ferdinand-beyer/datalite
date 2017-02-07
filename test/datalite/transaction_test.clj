(ns datalite.transaction-test
  (:require [clojure.test :refer :all]
            [datalite.connection :as conn]
            [datalite.database :as db]
            [datalite.schema :as schema]
            [datalite.test.util]
            [datalite.transaction :as dt :refer :all]))

(defn collect-tx-data
  ([tx] tx)
  ([tx form]
   (update tx :tx-data conj form)))

(deftest resolve-dbid-test
  (let [conn (conn/connect)
        db (db/db conn)]
    (testing "Non-existing integer partition id"
      (is (nil? (db/resolve-id db (->DbId schema/ident 1)))))
    (is (thrown-info? {:db/error :db.error/invalid-db-id}
                      (db/resolve-id db (->DbId :db.part/foo 1))))
    (is (= schema/ident
           (db/resolve-id db (->DbId schema/part-db schema/ident))))
    (is (= schema/ident
           (db/resolve-id db (->DbId :db.part/db schema/ident))))))

(deftest map-forms-to-list-forms-test
  (let [rf (@#'dt/expand-map-forms conj)]
    (is (= [:result] (rf [:result])))
    (is (= [[:db/add 1 2 3]]
           (rf [] [:db/add 1 2 3])))
    (is (= #{[:db/add 42 :a 1] [:db/add 42 :b 2]}
           (rf #{} {:db/id 42, :a 1, :b 2})))
    (is (= #{[:db/add :a 1] [:db/add :b 2]}
           (set (map (fn [[op e a v]] [op a v])
                     (rf [] {:a 1, :b 2})))))))

(deftest reversed-ref-attr-test
  (let [rf (@#'dt/reverse-refs conj)]
    (is (= [:result] (rf [:result])))
    (is (= [[:db/add 1 :x/y 2]]
           (rf [] [:db/add 1 :x/y 2])))
    (is (= [[:db/add 1 :x/y 2]]
           (rf [] [:db/add 2 :x/_y 1])))
    (is (= [[:db/add 1 :x/_y 2]]
           (rf [] [:db/add 2 :x/__y 1])))))

(deftest check-base-ops-test
  (let [rf (@#'dt/check-base-ops conj)]
    (is (= [:result] (rf [:result])))
    (is (= [[:db/add 1 :x/y 2]]
           (rf [] [:db/add 1 :x/y 2])))
    (is (= [[:db/retract 1 :x/y 2]]
           (rf [] [:db/retract 1 :x/y 2])))
    (is (= [[:db/add 1 :x/y 2]]
           (rf [] '(:db/add 1 :x/y 2))))
    (is (vector? (first (rf [] '(:db/add 1 :x/y 2)))))
    (is (thrown-info? {:db/error :db.error/invalid-tx-op}
                      (rf [] [])))
    (is (thrown-info? {:db/error :db.error/invalid-tx-op}
                      (rf [] [:db/add 1 :x/y])))
    (is (thrown-info? {:db/error :db.error/invalid-tx-op}
                      (rf [] [:db/add 1 :x/y 2 :too-many])))
    (is (thrown-info? {:db/error :db.error/invalid-tx-op}
                      (rf [] [:db/unknownOp 1 :x/y 2])))))

(deftest resolve-attributes-test
  (let [conn (conn/connect)
        tx (@#'dt/transaction conn)
        rf (@#'dt/resolve-attributes collect-tx-data)]
    (testing "a-value is replaced with resolved id"
      (is (= [[:db/add "foo" schema/ident :foo]]
             (:tx-data (rf tx [:db/add "foo" :db/ident :foo])))))
    (testing "resolved identifier is cached in tx"
      (is (= {:db/ident schema/ident}
             (:ids (rf tx [:db/add "foo" :db/ident :foo])))))
    (testing "attribute info is cached"
      (is (contains? (:attrs (rf tx [:db/add "foo" :db/ident :foo]))
                     schema/ident)))
    (testing "cache is used for resolution"
      (is (= [[:db/add 1 schema/doc 3]]
             (-> tx
                 (update :ids assoc :foo schema/doc)
                 (update :attrs assoc schema/doc
                         (get schema/system-attributes schema/doc))
                 (rf [:db/add 1 :foo 3])
                 :tx-data))))
    (testing "non-attribute entities are not accepted"
      (is (thrown-info?
            {:db/error :db.error/not-an-attribute
             :val :db.type/ref}
            (rf tx [:db/add 1 :db.type/ref 3]))))
    (testing "tempids are not accepted"
      (is (thrown-info?
            {:db/error :db.error/not-an-entity
             :val -1}
            (rf tx [:db/add 1 -1 3])))
      (is (thrown? IllegalArgumentException
                   (rf tx [:db/add 1 "attr" 3]))))
    (testing "resolution failure raises exception"
      (is (thrown-info?
            {:db/error :db.error/not-an-entity
             :val :foo/bar}
            (rf tx [:db/add 1 :foo/bar 3]))))))

(deftest resolve-entities-test
  (let [conn (conn/connect)
        tx (@#'dt/transaction conn)
        rf (@#'dt/resolve-entities collect-tx-data)]
    (testing "e-value is replaced with resolved id"
      (is (= [[:db/add schema/ident 2 3]]
             (:tx-data (rf tx [:db/add :db/ident 2 3])))))
    (testing "resolved identifier is cached in tx"
      (is (= {:db/ident schema/ident}
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

