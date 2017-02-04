(ns datalite.connection-test
  (:require [clojure.test :refer :all]
            [datalite.connection :refer :all]
            [datalite.bootstrap :as boot]))

(deftest connect-to-memory-db-test
  (with-open [conn (connect)]
    (is (boot/valid-schema? (.conn conn)))))

