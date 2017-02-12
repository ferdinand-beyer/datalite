(ns datalite.test-util
  "Test utilities."
  (:require [clojure.test :as test])
  (:import [java.nio.file Files Path]
           [java.nio.file.attribute FileAttribute]))

(defn ex-info-match?
  "Returns true if all entries in match are present in (ex-data ex)."
  [match ex]
  (let [data (ex-data ex)]
    (= match (select-keys data (keys match)))))

(defmethod test/assert-expr 'thrown-info?
  [msg form]
  ;; (is (thrown-info? match expr))
  ;; Asserts that evaluating expr throws an exception implementing
  ;; IExceptionInfo, with (ex-info-match? match ex) evaluating to true.
  (let [m (second form)
        body (nthnext form 2)]
    `(try ~@body
          (test/do-report {:type :fail, :message ~msg,
                           :expected '~form, :actual nil})
          (catch Exception e#
            (if (ex-info-match? ~m e#)
              (test/do-report {:type :pass, :message ~msg,
                               :expected '~form, :actual e#})
              (test/do-report {:type :fail, :message ~msg,
                               :expected '~form, :actual e#}))
            e#))))

(defn temp-dir
  "Creates a temporary directory and returns the java.nio.file.Path
  for it."
  [prefix]
  (Files/createTempDirectory prefix (make-array FileAttribute 0)))

