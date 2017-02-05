(ns datalite.database-test
  (:require [clojure.test :refer :all]
            [datalite.connection :as conn]
            [datalite.database :as db :refer :all]
            [datalite.schema :as schema]))

(deftest resolve-integer-entity-id-test
  (let [conn (conn/connect)
        db (db conn)]
    (is (nil? (resolve-id db -1)))
    (is (= schema/ident (resolve-id db schema/ident)))))

(deftest resolve-keyword-ident-test
  (let [conn (conn/connect)
        db (db conn)]
    (is (nil? (resolve-id db :foo)))
    (is (= schema/ident (resolve-id db :db/ident)))))

(deftest resolve-lookup-ref-test
  (let [conn (conn/connect)
        db (db conn)]
    (is (thrown? clojure.lang.ExceptionInfo
                 (resolve-id db [])))
    (is (thrown? clojure.lang.ExceptionInfo
                 (resolve-id db [:db/ident :db.part/db 0])))
    (is (thrown? clojure.lang.ExceptionInfo
                 (resolve-id db [:foo :bar])))
    (is (thrown? clojure.lang.ExceptionInfo
                 (resolve-id db [:db/valueType :db.type/keyword])))
    (is (= schema/part-db (resolve-id db [:db/ident :db.part/db])))
    (is (= schema/part-db (resolve-id db '(:db/ident :db.part/db))))
    (is (= schema/part-db (resolve-id db [schema/ident :db.part/db])))
    (is (= schema/part-db (resolve-id db [[:db/ident :db/ident] :db.part/db])))))

