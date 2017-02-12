(ns datalite.system-test
  (:require [clojure.test :refer :all]
            [datalite.system :as sys]))

(deftest all-entities-have-an-ident
  (doseq [[e attrs] sys/entities]
    (testing (str "entity " e)
      (is (keyword? (get attrs sys/ident))))))

(deftest all-attributes-are-installed
  (let [installed (get-in sys/entities [sys/part-db sys/install-attribute])]
    (doseq [[e attrs] sys/entities
            [a v] attrs]
      (is (contains? installed a)))))

(deftest all-value-types-are-installed
  (let [installed (get-in sys/entities [sys/part-db sys/install-value-type])]
    (doseq [[e attrs] sys/entities]
      (when-let [vt (get attrs sys/value-type)]
        (is (contains? installed vt))))))

(deftest all-attributes-have-value-type-and-cardinality
  (doseq [aid (get-in sys/entities [sys/part-db sys/install-attribute])]
    (is (contains? (get sys/entities aid) sys/value-type))
    (is (contains? (get sys/entities aid) sys/cardinality))))

