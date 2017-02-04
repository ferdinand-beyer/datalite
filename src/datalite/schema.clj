(ns datalite.schema
  (:require [clojure.set :refer [map-invert]]))

(def part-db 0)
(def part-tx 1)
(def part-user 2)

(def ident 10)

(def type-ref 11)
(def type-keyword 12)
(def type-long 13)
(def type-string 14)
(def type-boolean 15)
(def type-instant 16)
(def type-fn 17)
(def type-float 18)
(def type-double 19)
(def type-bytes 20)
(def type-bigint 21)
(def type-bigdec 22)
(def type-uuid 23)
(def type-uri 24)

(def cardinality-one 25)
(def cardinality-many 26)

(def unique-value 27)
(def unique-identity 28)

(def value-type 29)
(def cardinality 30)
(def unique 31)
(def index 32)
(def fulltext 33)
(def is-component 34)
(def no-history 35)
(def tx-instant 36)
(def doc 37)

(def system-attributes
  {part-db
   {ident :db.part/db
    doc "Name of the system partition. The system partition includes the core of Datalite, as well as user schemas: type definitions, attribute definitions, partition definitions, and data function definitions."}

   part-tx
   {ident :db.part/tx
    doc "Partition used to store data about transactions. Transaction data always includes a :db/txInstant which is the transaction's timestamp, and can be extended to store other information at transaction granularity."}

   part-user
   {ident :db.part/user
    doc "Name of the user partition. The user partition is analogous to the default namespace in a programming language, and should be used as a temporary home for data during interactive development."}

   ident
   {ident :db/ident
    value-type type-keyword
    cardinality cardinality-one
    unique unique-identity
    doc "Attribute used to uniquely name an entity."}

   type-ref
   {ident :db.type/ref
    doc "Value type for references. All references from one entity to another are through attributes with this value type."}

   type-keyword
   {ident :db.type/keyword
    doc "Value type for keywords. Keywords are used as names, and are interned for efficiency. Keywords map to the native interned-name type in languages that support them."}

   type-long
   {ident :db.type/long
    doc "Fixed integer value type. Same semantics as a Java long: 64 bits wide, two's complement binary representation."}

   type-string
   {ident :db.type/string
    doc "Value type for strings."}

   type-boolean
   {ident :db.type/boolean
    doc "Boolean value type."}

   type-instant
   {ident :db.type/instant
    doc "Value type for instants in time. Stored internally as a number of milliseconds since midnight, January 1, 1970 UTC. Representation type will vary depending on the language you are using."}

   type-fn
   {ident :db.type/fn
    doc "Value type for database functions."}

   type-float
   {ident :db.type/float
    doc "Floating point value type. Same semantics as a Java float: single-precision 32-bit IEEE 754 floating point."}

   type-double
   {ident :db.type/double
    doc "Floating point value type. Same semantics as a Java double: double-precision 64-bit IEEE 754 floating point."}

   type-bytes
   {ident :db.type/bytes
    doc "Value type for small binaries. Maps to byte array on the JVM."}

   type-bigint
   {ident :db.type/bigint
    doc "Value type for arbitrary precision integers. Maps to java.math.BigInteger on the JVM."}

   type-bigdec
   {ident :db.type/bigdec
    doc "Value type for arbitrary precision floating point numbers. Maps to java.math.BigDecimal on the JVM."}

   type-uuid
   {ident :db.type/uuid
    doc "Value type for UUIDs. Maps to java.util.UUID on the JVM."}

   type-uri
   {ident :db.type/uri
    doc "Value type for URIs. Maps to java.net.URI on the JVM."}

   cardinality-one
   {ident :db.cardinality/one
    doc "One of two legal values for the :db/cardinality attribute. Specify :db.cardinality/one for single-valued attributes, and :db.cardinality/many for many-valued attributes."}

   cardinality-many
   {ident :db.cardinality/many
    doc "One of two legal values for the :db/cardinality attribute. Specify :db.cardinality/one for single-valued attributes, and :db.cardinality/many for many-valued attributes."}

   unique-value
   {ident :db.unique/value
    doc "Specifies that an attribute's value is unique. Attempts to create a new entity with a colliding value for a :db.unique/value will fail."}

   unique-identity
   {ident :db.unique/identity
    doc "Specifies that an attribute's value is unique. Attempts to create a new entity with a colliding value for a :db.unique/identity will become upserts."}

   value-type
   {ident :db/valueType
    value-type type-ref
    cardinality cardinality-one
    doc "Property of an attribute that specifies the attribute's value type. Built-in value types include :db.type/keyword, :db.type/string, :db.type/ref, :db.type/instant, :db.type/long, :db.type/bigdec, :db.type/boolean, :db.type/float, :db.type/uuid, :db.type/double, :db.type/bigint, :db.type/uri."}

   cardinality
   {ident :db/cardinality
    value-type type-ref
    cardinality cardinality-one
    doc "Property of an attribute. Two possible values: :db.cardinality/one for single-valued attributes, and :db.cardinality/many for many-valued attributes. Defaults to :db.cardinality/one."}

   unique
   {ident :db/unique
    value-type type-ref
    cardinality cardinality-one
    doc "Property of an attribute. If value is :db.unique/value, then attribute value is unique to each entity. Attempts to insert a duplicate value for a temporary entity id will fail. If value is :db.unique/identity, then attribute value is unique, and upsert is enabled. Attempting to insert a duplicate value for a temporary entity id will cause all attributes associated with that temporary id to be merged with the entity already in the database. Defaults to nil."}

   index
   {ident :db/index
    value-type type-boolean
    cardinality cardinality-one
    doc "Property of an attribute. If true, create an AVET index for the attribute. Defaults to false."}

   fulltext
   {ident :db/fulltext
    doc "Property of an attribute. If true, create a fulltext search index for the attribute. Defaults to false."}

   is-component
   {ident :db/isComponent
    doc "Property of attribute whose value type is :db.type/ref. If true, then the attribute is a component of the entity referencing it. When you query for an entire entity, components are fetched automatically. Defaults to nil."}

   no-history
   {ident :db/noHistory
    doc "Property of an attribute. If true, past values of the attribute are not retained after indexing. Defaults to false."}

   tx-instant
   {ident :db/txInstant
    index true
    doc "Attribute whose value is a :db.type/instant. A :db/txInstant is recorded automatically with every transaction."}

   doc
   {ident :db/doc
    fulltext true
    doc "Documentation string for an entity."}})

(def system-keys (into {} (map (fn [[k attrs]]
                                 [k (get attrs ident)])
                               system-attributes)))

(def system-ids (map-invert system-keys))

