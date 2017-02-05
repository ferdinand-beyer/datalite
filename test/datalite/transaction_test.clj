(ns datalite.transaction-test
  (:require [clojure.test :refer :all]
            [datalite.connection :as conn]
            [datalite.database :as db]
            [datalite.schema :as schema]
            [datalite.transaction :as dt :refer :all]))

(defn collect-tx-data
  ([tx] tx)
  ([tx form]
   (update tx :tx-data conj form)))

(deftest resolve-tempid-test
  (let [conn (conn/connect)
        db (db/db conn)]
    (testing "Non-existing integer partition id"
      (is (nil? (db/resolve-id db (tempid schema/ident 1)))))
    (is (thrown? clojure.lang.ExceptionInfo
                 (db/resolve-id db (tempid :db.part/foo 1))))
    (is (= schema/ident
           (db/resolve-id db (tempid schema/part-db schema/ident))))
    (is (= schema/ident
           (db/resolve-id db (tempid :db.part/db schema/ident))))))

(deftest map-forms-to-list-forms-test
  (let [xform (@#'dt/expand-map-forms conj)]
    (is (= [1] (xform [1])))
    (is (= [[:db/add 1 2 3]]
           (xform [] [:db/add 1 2 3])))
    (is (= #{[:db/add 42 :a 1] [:db/add 42 :b 2]}
           (xform #{} {:db/id 42, :a 1, :b 2})))
    (is (= #{[:db/add :a 1] [:db/add :b 2]}
           (set (map (fn [[op e a v]] [op a v])
                     (xform [] {:a 1, :b 2})))))))

(deftest reversed-ref-attr-test
  (let [xform (@#'dt/reverse-refs conj)]
    (is (= [1] (xform [1])))
    (is (= [[:db/add 1 :x/y 2]]
           (xform [] [:db/add 1 :x/y 2])))
    (is (= [[:db/add 1 :x/y 2]]
           (xform [] [:db/add 2 :x/_y 1])))
    (is (= [[:db/add 1 :x/_y 2]]
           (xform [] [:db/add 2 :x/__y 1])))))

(deftest check-base-ops-test
  (let [xform (@#'dt/check-base-ops conj)]
    (is (= [1] (xform [1])))
    (is (= [[:db/add 1 :x/y 2]]
           (xform [] [:db/add 1 :x/y 2])))
    (is (= [[:db/retract 1 :x/y 2]]
           (xform [] [:db/retract 1 :x/y 2])))
    (is (= [[:db/add 1 :x/y 2]]
           (xform [] '(:db/add 1 :x/y 2))))
    (is (vector? (first (xform [] '(:db/add 1 :x/y 2)))))
    (is (thrown? clojure.lang.ExceptionInfo
                 (xform [] [])))
    (is (thrown? clojure.lang.ExceptionInfo
                 (xform [] [:db/add 1 :x/y])))
    (is (thrown? clojure.lang.ExceptionInfo
                 (xform [] [:db/add 1 :x/y 2 :too-many])))
    (is (thrown? clojure.lang.ExceptionInfo
                 (xform [] [:db/unknownOp 1 :x/y 2])))))

(deftest resolve-attributes-test
  (let [conn (conn/connect)
        tx (@#'dt/transaction conn)
        xform (@#'dt/resolve-attributes collect-tx-data)]
    (testing "a-value is replaced with resolved id"
      (is (= [[:db/add "foo" schema/ident :foo]]
             (:tx-data
               (xform tx [:db/add "foo" :db/ident :foo])))))
    (testing "resolved identifier is cached in tx"
      (is (= {:db/ident schema/ident}
             (:ids
               (xform tx [:db/add "foo" :db/ident :foo])))))
    (testing "attribute info is cached"
      (is (contains?
            (:attrs
              (xform tx [:db/add "foo" :db/ident :foo]))
            schema/ident)))
    (testing "cache is used for resolution"
      (is (= [[:db/add 1 schema/doc 3]]
             (:tx-data
               (xform (-> tx
                          (update :ids assoc :foo schema/doc)
                          (update :attrs assoc schema/doc
                                  (get schema/system-attributes schema/doc)))
                      [:db/add 1 :foo 3])))))
    (testing "non-attribute entities are not accepted"
      (is (thrown? clojure.lang.ExceptionInfo
                   (xform tx [:db/add 1 :db.type/ref 3]))))
    (testing "tempids are not accepted"
      (is (thrown? clojure.lang.ExceptionInfo
                   (xform tx [:db/add 1 -1 3])))
      (is (thrown? RuntimeException
                   (xform tx [:db/add 1 "attr" 3]))))
    (testing "resolution failure raises exception"
      (is (thrown? clojure.lang.ExceptionInfo
                   (xform tx [:db/add 1 :foo/bar 3]))))))

(deftest resolve-entities-test
  (let [conn (conn/connect)
        tx (@#'dt/transaction conn)
        xform (@#'dt/resolve-entities collect-tx-data)]
    (testing "e-value is replaced with resolved id"
      (is (= [[:db/add schema/ident 2 3]]
             (:tx-data
               (xform tx [:db/add :db/ident 2 3])))))
    (testing "resolved identifier is cached in tx"
      (is (= {:db/ident schema/ident}
             (:ids
               (xform tx [:db/add :db/ident 2 3])))))
    (testing "cache is used for resolution"
      (is (= [[:db/add 42 2 3]]
             (:tx-data
               (xform (update tx :ids assoc :foo 42)
                      [:db/add :foo 2 3])))))
    (testing "tempids are collected but left alone"
      (is (= [[:db/add -1 2 3]]
             (:tx-data
               (xform tx [:db/add -1 2 3]))))
      (is (= #{-1}
             (:tempids
               (xform tx [:db/add -1 2 3]))))
      (is (= {-1 -1}
             (:ids
               (xform tx [:db/add -1 2 3])))))
    (testing "resolution failure raises exception"
      (is (thrown? clojure.lang.ExceptionInfo
                   (xform tx [:db/add :foo/bar 2 3]))))))
