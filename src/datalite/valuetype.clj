(ns datalite.valuetype
  (:require [datalite.system :as sys])
  (:import [clojure.lang BigInt Keyword]
           [java.math BigDecimal BigInteger]
           [java.util Date UUID]
           [java.net URI]))

(def ByteArray (Class/forName "[B"))

(def types
  {sys/type-keyword
   {:class Keyword
    :read (fn [^String s] (keyword (subs s 1)))
    :write (fn [^Keyword k] (.toString k))}

   sys/type-string
   {:class String
    :read str
    :write str}

   sys/type-boolean
   {:class Boolean
    :read (fn [n] (not (zero? n)))
    :write (fn [b] (if b 1 0))}

   sys/type-long
   {:class Long
    :read long
    :write long}

   sys/type-bigint
   {:class BigInt
    :read (fn [s] (bigint s))
    :write (fn [i] (.toString (bigint i)))}

   sys/type-float
   {:class Float
    :read float
    :write double}

   sys/type-double
   {:class Double
    :read double
    :write double}

   sys/type-bigdec
   {:class BigDecimal
    :read (fn [^String s] (BigDecimal. s))
    :write (fn [^BigDecimal i] (.toString i))}

   sys/type-ref
   {:class Long
    :read long
    :write long}

   sys/type-instant
   {:class Date
    :read (fn [^long t] (Date. t))
    :write (fn [^Date d] (.getTime d))}

   sys/type-uuid
   {:class UUID
    :read (fn [^String s] (UUID/fromString s))
    :write (fn [^UUID u] (.toString u))}

   sys/type-uri
   {:class URI
    :read (fn [^String s] (URI. s))
    :write (fn [^URI u] (.toString u))}

   sys/type-bytes
   {:class ByteArray
    :read bytes
    :write bytes}})

(defprotocol ValueType
  (value-type [x]))

(extend-protocol ValueType
  Keyword
    (value-type [_] sys/type-keyword)
  String
    (value-type [_] sys/type-string)
  Boolean
    (value-type [_] sys/type-boolean)
  Integer
    (value-type [_] sys/type-long)
  Long
    (value-type [_] sys/type-long)
  BigInt
    (value-type [_] sys/type-bigint)
  BigInteger
    (value-type [_] sys/type-bigint)
  Float
    (value-type [_] sys/type-float)
  Double
    (value-type [_] sys/type-double)
  BigDecimal
    (value-type [_] sys/type-bigdec)
  Date
    (value-type [_] sys/type-instant)
  UUID
    (value-type [_] sys/type-uuid)
  URI
    (value-type [_] sys/type-uri))

(extend-protocol ValueType
  (Class/forName "[B")
    (value-type [_] sys/type-bytes))

(defn coerce-read
  [vt x]
  ((get-in types [vt :read]) x))

(defn coerce-write
  [vt x]
  ((get-in types [vt :write]) x))

