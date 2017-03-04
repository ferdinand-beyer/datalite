(ns ^:integration datalite.api-test
  (require [clojure.test :refer :all]
           [datalite.api :as d]))

(deftest attribute-info-of-builtin-ident
  (let [conn (d/connect)
        db (d/db conn)
        a (d/attribute db :db/ident)]
    (is (associative? a))
    (is (integer? (:id a)))
    (is (= :db/ident (:ident a)))
    (is (= :db.cardinality/one (:cardinality a)))
    (is (= :db.type/keyword (:value-type a)))
    (is (= :db.unique/identity (:unique a)))
    (is (false? (:indexed a)))
    (is (true? (:has-avet a)))
    (is (false? (:no-history a)))
    (is (false? (:is-component a)))
    (is (false? (:fulltext a)))))

