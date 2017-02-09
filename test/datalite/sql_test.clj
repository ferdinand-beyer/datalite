(ns datalite.sql-test
  (:require [clojure.test :refer :all]
            [datalite.sql :refer :all]
            [datalite.test.util])
  (:import [java.sql Connection DriverManager]))

(defn connect
  []
  (DriverManager/getConnection "jdbc:sqlite::memory:"))

(defn sample-db
  []
  (let [con (connect)]
    (with-open [stmt (.createStatement con)]
      (doto stmt
        (.addBatch (str "CREATE TABLE characters ("
                        " id INTEGER PRIMARY KEY,"
                        " name TEXT NOT NULL"
                        ")"))
        (.addBatch "INSERT INTO characters VALUES (NULL, 'Homer')")
        (.addBatch "INSERT INTO characters VALUES (NULL, 'Marge')")
        (.addBatch "INSERT INTO characters VALUES (NULL, 'Bart')")
        (.addBatch "INSERT INTO characters VALUES (NULL, 'Lisa')")
        (.addBatch "INSERT INTO characters VALUES (NULL, 'Maggie')")
        (.executeBatch)))
    con))

(deftest query-test
  (with-open [con (sample-db)]
    (is (= [[1 "Homer"]
            [2 "Marge"]
            [3 "Bart"]
            [4 "Lisa"]
            [5 "Maggie"]]
           (query con "SELECT * FROM characters")))
    (with-open [stmt (.prepareStatement con "SELECT * FROM characters")]
      (is (= [[1 "Homer"]
              [2 "Marge"]
              [3 "Bart"]
              [4 "Lisa"]
              [5 "Maggie"]]
             (query con stmt))))
    (is (= [["Bart"] ["Lisa"] ["Maggie"]]
           (query con "SELECT name FROM characters WHERE id >= ?" [3])))
    (with-open [stmt (.prepareStatement con "SELECT name FROM characters WHERE id = ?")]
      (is (= [["Maggie"]]
             (query con stmt [5]))))))

(deftest query-first-test
  (with-open [con (sample-db)]
    (is (= [1 "Homer"]
           (query-first con "SELECT * FROM characters")))))

(deftest query-one-test
  (with-open [con (sample-db)]
    (is (= [4 "Lisa"]
           (query-one con "SELECT * FROM characters WHERE name LIKE ?" ["L%"])))
    (is (thrown? RuntimeException
                 (query-one con "SELECT * FROM characters")))
    (is (thrown? RuntimeException
                 (query-one con "SELECT * FROM characters WHERE ? = ?" [1 0])))))

(deftest query-value-test
  (with-open [con (sample-db)]
    (is (= "Lisa"
           (query-value con "SELECT name FROM characters WHERE id = 4")))
    (is (thrown? RuntimeException
                 (query-value con "SELECT name FROM characters")))
    (is (thrown? RuntimeException
                 (query-value con "SELECT name FROM characters WHERE 1 = 0")))
    (is (thrown? RuntimeException
                 (query-value con "SELECT * FROM characters WHERE id = 4")))))

(deftest exec-many-test
  (with-open [con (connect)]
    (is (= 1 (exec-many! con ["CREATE TABLE kvs (k, v)"
                              "CREATE UNIQUE INDEX kvs_k ON kvs(k)"
                              "INSERT INTO kvs VALUES ('foo', 'bar')"])))))

(deftest insert-sql-test
  (is (= "INSERT INTO \"foo\" VALUES (?)"
         (insert-sql "foo" 1)))
  (is (= "INSERT INTO \"foo\" (\"foo\", \"bar\") VALUES (?, ?)"
         (insert-sql "foo" ["foo" "bar"])))
  (is (= "INSERT INTO \"tbl\" (\"kwd\", \"name\") VALUES (?, ?)"
         (insert-sql "tbl" [:kwd :ns/name]))))

(deftest update-sql-test
  (is (= "UPDATE \"foo\" SET \"bar\" = ?"
         (update-sql "foo" ["bar"])))
  (is (= "UPDATE \"tbl\" SET \"c1\" = ?, \"c2\" = ?, \"c3\" = ?"
         (update-sql "tbl" ["c1" "c2" "c3"])))
  (is (= "UPDATE \"tbl\" SET \"c1\" = ?, \"c2\" = ?, \"c3\" = ?"
         (update-sql "tbl" ["c1" :ns/c2 'c3])))
  (is (= "UPDATE \"tbl\" SET \"c1\" = ?, \"c2\" = ? WHERE c1 = c2"
         (update-sql "tbl" ["c1" "c2"] "c1 = c2"))))

(deftest insert-test
  (with-open [con (sample-db)]
    (is (= 1 (insert! con "characters" {:name "Superman"})))
    (is (= [[6 "Superman"]]
           (query con "SELECT * FROM characters WHERE id > 5")))))

(deftest insert-many-test
  (with-open [con (sample-db)]
    (is (= 3 (insert-many! con "characters" [:name]
                           [["Barney"]
                            ["Lenny"]
                            ["Carl"]])))
    (is (= [[6 "Barney"]
            [7 "Lenny"]
            [8 "Carl"]]
           (query con "SELECT * FROM characters WHERE id > 5")))))

(deftest update-test
  (with-open [con (sample-db)]
    (is (= 5 (update! con "characters" {:name "John"})))
    (is (= (repeat 5 ["John"])
           (query con "SELECT name FROM characters"))))
  (with-open [con (sample-db)]
    (is (= 1 (update! con "characters" {:name "John"} "name = 'Bart'")))
    (is (= [["John"]]
           (query con "SELECT name FROM characters WHERE id = 3"))))
  (with-open [con (sample-db)]
    (is (= 1 (update! con "characters" {:name "John"} "name = ?" ["Bart"])))
    (is (= [["John"]]
           (query con "SELECT name FROM characters WHERE id = 3"))))
  (with-open [con (sample-db)]
    (is (= 1 (update! con "characters" {:name "John"} {:name "Bart"})))
    (is (= [["John"]]
           (query con "SELECT name FROM characters WHERE id = 3")))))