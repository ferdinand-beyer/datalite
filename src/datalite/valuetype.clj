(ns datalite.valuetype
  (:require [datalite.schema :as schema])
  (:import [clojure.lang BigInt Keyword]
           [java.math BigDecimal BigInteger]
           [java.util Date UUID]
           [java.net URI]))

(def ByteArray (Class/forName "[B"))

(def types
  {schema/type-keyword
   {:class Keyword
    :read (fn [^String s] (keyword (subs s 1)))
    :write (fn [^Keyword k] (.toString k))}

   schema/type-string
   {:class String
    :read str
    :write str}

   schema/type-boolean
   {:class Boolean
    :read (fn [n] (not (zero? n)))
    :write (fn [b] (if b 1 0))}

   schema/type-long
   {:class Long
    :read long
    :write long}

   schema/type-bigint
   {:class BigInt
    :read (fn [s] (bigint s))
    :write (fn [i] (.toString (bigint i)))}

   schema/type-float
   {:class Float
    :read float
    :write double}

   schema/type-double
   {:class Double
    :read double
    :write double}

   schema/type-bigdec
   {:class BigDecimal
    :read (fn [^String s] (BigDecimal. s))
    :write (fn [^BigDecimal i] (.toString i))}

   schema/type-ref
   {:class Long
    :read long
    :write long}

   schema/type-instant
   {:class Date
    :read (fn [^long t] (Date. t))
    :write (fn [^Date d] (.getTime d))}

   schema/type-uuid
   {:class UUID
    :read (fn [^String s] (UUID/fromString s))
    :write (fn [^UUID u] (.toString u))}

   schema/type-uri
   {:class URI
    :read (fn [^String s] (URI. s))
    :write (fn [^URI u] (.toString u))}

   schema/type-bytes
   {:class ByteArray
    :read bytes
    :write bytes}})

(defprotocol ValueType
  (value-type [x]))

(extend-protocol ValueType
  Keyword
    (value-type [_] schema/type-keyword)
  String
    (value-type [_] schema/type-string)
  Boolean
    (value-type [_] schema/type-boolean)
  Integer
    (value-type [_] schema/type-long)
  Long
    (value-type [_] schema/type-long)
  BigInt
    (value-type [_] schema/type-bigint)
  BigInteger
    (value-type [_] schema/type-bigint)
  Float
    (value-type [_] schema/type-float)
  Double
    (value-type [_] schema/type-double)
  BigDecimal
    (value-type [_] schema/type-bigdec)
  Date
    (value-type [_] schema/type-instant)
  UUID
    (value-type [_] schema/type-uuid)
  URI
    (value-type [_] schema/type-uri))

(extend-protocol ValueType
  (Class/forName "[B")
    (value-type [_] schema/type-bytes))

(defn coerce-read
  [vt x]
  ((get-in types [vt :read]) x))

(defn coerce-write
  [vt x]
  ((get-in types [vt :write]) x))

