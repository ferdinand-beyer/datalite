(ns datalite.bootstrap-test
  (:require [clojure.test :refer :all]
            [datalite.bootstrap :refer :all]
            [datalite.schema :refer [system-ids]])
  (:import [java.sql Connection DriverManager]))

(defn- memory-connection
  []
  (DriverManager/getConnection "jdbc:sqlite::memory:"))

(defn- exec
  [conn sql]
  (with-open [stmt (.createStatement conn)]
    (.execute stmt sql)))

(deftest table-exists-test
  (let [conn (memory-connection)]
    (exec conn "CREATE TABLE tbl1 (col INTEGER)")
    (is (true? (@#'datalite.bootstrap/table-exists? conn "tbl1")))
    (is (false? (@#'datalite.bootstrap/table-exists? conn "tbl2")))))

(deftest schema-creation-test
  (let [conn (memory-connection)]
    (is (false? (schema-exists? conn)))
    (create-schema conn)
    (is (true? (schema-exists? conn)))))

(deftest empty-schema-invalid-version-test
  (let [conn (memory-connection)]
    (create-schema conn)
    (is (false? (valid-version? conn)))))

(deftest bootstrap-meta-test
  (let [conn (memory-connection)]
    (create-schema conn)
    (bootstrap-meta conn)
    (is (true? (valid-version? conn)))))

(deftest initial-head-numbers-test
  (let [conn (memory-connection)]
    (create-schema conn)
    (with-open [stmt (.createStatement conn)
                rs (.executeQuery stmt "SELECT * FROM head")]
      (is (false? (.next rs))))
    (bootstrap-head conn)
    (let [max-system-id (apply max (vals system-ids))]
      (with-open [stmt (.createStatement conn)
                  rs (.executeQuery stmt "SELECT * FROM head")]
        (is (true? (.next rs)))
        (is (> (.getLong rs 1) max-system-id))
        (is (= 1000 (.getLong rs 2)))
        (is (false? (.next rs)))))))

