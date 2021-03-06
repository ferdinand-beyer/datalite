(ns datalite.bootstrap-test
  (:require [clojure.test :refer :all]
            [datalite.bootstrap :as b :refer :all]
            [datalite.system :as sys])
  (:import [java.sql Connection DriverManager]))

(defn- memory-connection
  []
  (DriverManager/getConnection "jdbc:sqlite::memory:"))

(defn- exec
  [con sql]
  (with-open [stmt (.createStatement con)]
    (.execute stmt sql)))

(deftest table-exists-test
  (let [con (memory-connection)]
    (exec con "CREATE TABLE tbl1 (col INTEGER)")
    (is (true? (@#'b/table-exists? con "tbl1")))
    (is (false? (@#'b/table-exists? con "tbl2")))))

(deftest schema-creation-test
  (let [con (memory-connection)]
    (is (false? (schema-exists? con)))
    (create-schema! con)
    (is (true? (schema-exists? con)))))

(deftest empty-schema-invalid-version-test
  (let [con (memory-connection)]
    (create-schema! con)
    (is (false? (valid-version? con)))))

(deftest populate-meta-test
  (let [con (memory-connection)]
    (create-schema! con)
    (populate-meta! con)
    (is (true? (valid-version? con)))))

(deftest initial-head-numbers-test
  (let [con (memory-connection)]
    (create-schema! con)
    (with-open [stmt (.createStatement con)
                rs (.executeQuery stmt "SELECT * FROM head")]
      (is (false? (.next rs))))
    (populate-head! con)
    (let [max-system-id (apply max (keys sys/entities))]
      (with-open [stmt (.createStatement con)
                  rs (.executeQuery stmt "SELECT * FROM head")]
        (is (true? (.next rs)))
        (is (> (.getLong rs 1) max-system-id))
        (is (= 1000 (.getLong rs 2)))
        (is (false? (.next rs)))))))

(deftest schema-entity-tuples-test
  (let [schema-tuples @#'b/schema-tuples]
    (is (= [] (schema-tuples {})))
    (is (= [[1 2 3]]
           (schema-tuples {1 {2 3}})))
    (is (= [[:e :a1 1]
            [:e :a2 2]]
           (schema-tuples {:e {:a1 1, :a2 2}})))
    (is (= [[:e1 :a1 1]
            [:e1 :a2 2]
            [:e2 :a1 3]]
           (schema-tuples {:e1 {:a1 1, :a2 2}
                           :e2 {:a1 3}})))
    (is (= [[:e :a 1]
            [:e :a 2]]
           (schema-tuples {:e {:a [1 2]}})))
    (is (= #{[:e :a 1]
             [:f :b 2]
             [:f :b 3]}
           (set (schema-tuples {:e {:a 1}
                                :f {:b #{2 3}}}))))))

