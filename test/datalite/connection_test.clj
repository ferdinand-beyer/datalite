(ns datalite.connection-test
  (:require [clojure.test :refer :all]
            [datalite.connection :as c :refer :all]
            [datalite.bootstrap :as boot]
            [datalite.sql :as sql]
            [datalite.test-util :as util]))

(deftest connect-to-memory-db-test
  (with-open [conn (connect)]
    (is (boot/valid-schema? (sql-con conn)))))

(deftest ^:integration connect-to-file-test
  (let [temp-dir (util/temp-dir (str *ns*))]
    (let [filepath (.resolve temp-dir "new.db")]
      (testing "create a new database file"
        (with-open [conn (connect (str filepath))]
          (is (boot/valid-schema? (sql-con conn))))
        (is (.exists (.toFile filepath))))
      (testing "re-open existing database file"
        (with-open [conn (connect (str filepath))]
          (is (boot/valid-schema? (sql-con conn)))))
      (with-open [con (@#'c/sqlite-connect (str filepath))]
        (sql/exec! con "DELETE FROM meta"))
      (testing "open database file with no meta information"
        (is (thrown-info? {:db/error :db.error/unsupported-schema}
                          (connect (str filepath))))))))

