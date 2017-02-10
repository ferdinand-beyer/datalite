(ns datalite.valuetype-test
  (:require [clojure.test :refer :all]
            [datalite.schema :as schema]
            [datalite.valuetype :as vt :refer :all])
  (:import [clojure.lang Keyword]
           [java.math BigDecimal BigInteger]
           [java.util Date UUID]
           [java.net URI]))

(def values
  {schema/type-ref schema/ident
   schema/type-keyword ::kwd
   schema/type-long 42
   schema/type-string "H.E. Pennypacker"
   schema/type-boolean true
   schema/type-instant (Date.)
   #_ schema/type-fn
   schema/type-float (float 3.1415)
   schema/type-double 3.1415
   schema/type-bytes (byte-array 10)
   schema/type-bigint (BigInteger. "12345678901234567890")
   schema/type-bigdec (BigDecimal. "1234567890.1234567890")
   schema/type-uuid (UUID/randomUUID)
   schema/type-uri (URI. "https://github.com/ferdinand-beyer/datalite")})

(def sqlite-types
  {schema/type-ref Long
   schema/type-keyword String
   schema/type-long Long
   schema/type-string String
   schema/type-boolean Long
   schema/type-instant Long
   #_ schema/type-fn
   schema/type-float Double
   schema/type-double Double
   schema/type-bytes ByteArray
   schema/type-bigint String
   schema/type-bigdec String
   schema/type-uuid String
   schema/type-uri String})

(deftest sqlite-types-test
  (doseq [[vt cls] sqlite-types]
    (testing (str "Value type " vt " " (get schema/system-keys vt))
      (let [value (get values vt)
            ->sql (get-in vt/types [vt :write])]
        (is (= cls (class (->sql value))))))))

(deftest write-read-test
  (doseq [[vt value] values]
    (testing (str "Value type " vt " " (get schema/system-keys vt))
      (let [{:keys [read write]} (get vt/types vt)]
        (is (= value (read (write value))))))))

