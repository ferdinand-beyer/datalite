(ns datalite.valuetype-test
  (:require [clojure.test :refer :all]
            [datalite.schema :as schema]
            [datalite.system :as sys]
            [datalite.valuetype :as vt :refer :all])
  (:import [clojure.lang Keyword]
           [java.math BigDecimal BigInteger]
           [java.util Date UUID]
           [java.net URI]))

(def values
  {sys/type-ref sys/ident
   sys/type-keyword ::kwd
   sys/type-long 42
   sys/type-string "H.E. Pennypacker"
   sys/type-boolean true
   sys/type-instant (Date.)
   #_ sys/type-fn
   sys/type-float (float 3.1415)
   sys/type-double 3.1415
   sys/type-bytes (byte-array 10)
   sys/type-bigint 12345678901234567890N
   sys/type-bigdec 1234567890.1234567890M
   sys/type-uuid (UUID/randomUUID)
   sys/type-uri (URI. "https://github.com/ferdinand-beyer/datalite")})

(def sqlite-types
  {sys/type-ref Long
   sys/type-keyword String
   sys/type-long Long
   sys/type-string String
   sys/type-boolean Long
   sys/type-instant Long
   #_ sys/type-fn
   sys/type-float Double
   sys/type-double Double
   sys/type-bytes ByteArray
   sys/type-bigint String
   sys/type-bigdec String
   sys/type-uuid String
   sys/type-uri String})

(deftest sqlite-types-test
  (doseq [[vt cls] sqlite-types]
    (testing (str "Value type " vt " " (schema/ident schema/system-schema vt))
      (let [value (get values vt)
            ->sql (get-in vt/types [vt :write])]
        (is (= cls (class (->sql value))))))))

(deftest write-read-test
  (doseq [[vt value] values]
    (testing (str "Value type " vt " " (schema/ident schema/system-schema vt))
      (let [{:keys [read write]} (get vt/types vt)]
        (is (= value (read (write value))))))))

