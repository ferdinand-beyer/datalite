(ns datalite.test.util
  "Test utilities."
  (:require [clojure.test :as test]))

(defmethod test/assert-expr 'thrown-with-data?
  [msg form]
  ;; (is (thrown-with-data? map expr)
  ;; Asserts that evaluating expr throws an exception
  ;; implementing IExceptionInfo, with every entry in
  ;; map being present in (ex-data thrown-exception).
  (let [m (second form)
        body (nthnext form 2)]
    `(try ~@body
          (ct/do-report {:type :fail, :message ~msg,
                         :expected '~form, :actual nil})
          (catch Exception e#
            (if (= ~m (select-keys (ex-data e#) (keys ~m)))
              (ct/do-report {:type :pass, :message ~msg,
                             :expected '~form, :actual e#})
              (ct/do-report {:type :fail, :message ~msg,
                             :expected '~form, :actual e#}))
            e#))))

