(ns datalite.schema-test
  (:require [clojure.test :refer :all]
            [datalite.schema :refer :all]
            [datalite.system :as sys]))

(deftest attr-info-test
  (let [attr (attr-info system-schema sys/ident)]
    (is (= sys/ident (:id attr)))
    (is (= :db/ident (:ident attr)))
    (is (= :db.cardinality/one (:cardinality attr)))
    (is (= :db.type/keyword (:value-type attr)))
    (is (= :db.unique/identity (:unique attr)))
    (is (false? (:indexed attr)))
    (is (true? (:has-avet attr)))
    (is (false? (:no-history attr)))
    (is (false? (:is-component attr)))
    (is (false? (:fulltext attr)))))

