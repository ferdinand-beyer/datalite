(ns datalite.system
  "System entities.")

(def part-db 0)
(def part-tx 1)
(def part-user 2)

(def ident 10)

(def install-partition 11)
(def install-value-type 12)
(def install-attribute 13)
(def install-function 14)

(def type-ref 20)
(def type-keyword 21)
(def type-long 22)
(def type-string 23)
(def type-boolean 24)
(def type-instant 25)
(def type-fn 26)
(def type-float 27)
(def type-double 28)
(def type-bytes 29)
(def type-bigint 30)
(def type-bigdec 31)
(def type-uuid 32)
(def type-uri 33)

(def cardinality-one 35)
(def cardinality-many 36)

(def unique-value 37)
(def unique-identity 38)

(def value-type 40)
(def cardinality 41)
(def unique 42)
(def index 43)
(def fulltext 44)
(def is-component 45)
(def no-history 46)
(def tx-instant 47)
(def doc 48)

(def entities
  {part-db
   {ident :db.part/db
    doc (str "Name of the system partition. The system partition "
             "includes the core of Datalite, as well as user "
             "schemas: type definitions, attribute definitions, "
             "partition definitions, and data function definitions.")}

   part-tx
   {ident :db.part/tx
    doc (str "Partition used to store data about transactions. "
             "Transaction data always includes a :db/txInstant which "
             "is the transaction's timestamp, and can be extended to "
             "store other information at transaction granularity.")}

   part-user
   {ident :db.part/user
    doc (str "Name of the user partition. The user partition is "
             "analogous to the default namespace in a programming "
             "language, and should be used as a temporary home for "
             "data during interactive development.")}

   ident
   {ident :db/ident
    value-type type-keyword
    cardinality cardinality-one
    unique unique-identity
    doc "Attribute used to uniquely name an entity."}

   install-partition
   {ident :db.install/partition
    value-type type-ref
    cardinality cardinality-many
    doc (str "System attribute with type :db.type/ref. Asserting this "
             "attribute on :db.part/db with value v will install v as "
             "a partition.")}

   install-value-type
   {ident :db.install/valueType
    value-type type-ref
    cardinality cardinality-many
    doc (str "System attribute with type :db.type/ref. Asserting this "
             "attribute on :db.part/db with value v will install v as "
             "a value type.")}

   install-attribute
   {ident :db.install/attribute
    value-type type-ref
    cardinality cardinality-many
    doc (str "System attribute with type :db.type/ref. Asserting this "
             "attribute on :db.part/db with value v will install v as "
             "an attribute.")}

   install-function
   {ident :db.install/function
    value-type type-ref
    cardinality cardinality-many
    doc (str "System attribute with type :db.type/ref. Asserting this "
             "attribute on :db.part/db with value v will install v as "
             "a data function.")}

   type-ref
   {ident :db.type/ref
    doc (str "Value type for references. All references from one "
             "entity to another are through attributes with this "
             "value type.")}

   type-keyword
   {ident :db.type/keyword
    doc (str "Value type for keywords. Keywords are used as names, "
             "and are interned for efficiency. Keywords map to the "
             "native interned-name type in languages that support them.")}

   type-long
   {ident :db.type/long
    doc (str "Fixed integer value type. Same semantics as a Java long: "
             "64 bits wide, two's complement binary representation.")}

   type-string
   {ident :db.type/string
    doc "Value type for strings."}

   type-boolean
   {ident :db.type/boolean
    doc "Boolean value type."}

   type-instant
   {ident :db.type/instant
    doc (str "Value type for instants in time. Stored internally as "
             "a number of milliseconds since midnight, January 1, "
             "1970 UTC. Representation type will vary depending on "
             "the language you are using.")}

   type-fn
   {ident :db.type/fn
    doc "Value type for database functions."}

   type-float
   {ident :db.type/float
    doc (str "Floating point value type. Same semantics as a Java float: "
             "single-precision 32-bit IEEE 754 floating point.")}

   type-double
   {ident :db.type/double
    doc (str "Floating point value type. Same semantics as a Java double: "
             "double-precision 64-bit IEEE 754 floating point.")}

   type-bytes
   {ident :db.type/bytes
    doc "Value type for small binaries. Maps to byte array on the JVM."}

   type-bigint
   {ident :db.type/bigint
    doc (str "Value type for arbitrary precision integers. Maps to "
             "java.math.BigInteger on the JVM.")}

   type-bigdec
   {ident :db.type/bigdec
    doc (str "Value type for arbitrary precision floating point numbers. "
             "Maps to java.math.BigDecimal on the JVM.")}

   type-uuid
   {ident :db.type/uuid
    doc "Value type for UUIDs. Maps to java.util.UUID on the JVM."}

   type-uri
   {ident :db.type/uri
    doc "Value type for URIs. Maps to java.net.URI on the JVM."}

   cardinality-one
   {ident :db.cardinality/one
    doc (str "One of two legal values for the :db/cardinality attribute. "
             "Specify :db.cardinality/one for single-valued attributes, "
             "and :db.cardinality/many for many-valued attributes.")}

   cardinality-many
   {ident :db.cardinality/many
    doc (str "One of two legal values for the :db/cardinality attribute. "
             "Specify :db.cardinality/one for single-valued attributes, "
             "and :db.cardinality/many for many-valued attributes.")}

   unique-value
   {ident :db.unique/value
    doc (str "Specifies that an attribute's value is unique. Attempts to "
             "create a new entity with a colliding value for a "
             ":db.unique/value will fail.")}

   unique-identity
   {ident :db.unique/identity
    doc (str "Specifies that an attribute's value is unique. Attempts to "
             "create a new entity with a colliding value for a "
             ":db.unique/identity will become upserts.")}

   value-type
   {ident :db/valueType
    value-type type-ref
    cardinality cardinality-one
    doc (str "Property of an attribute that specifies the attribute's "
             "value type. Built-in value types include :db.type/keyword, "
             ":db.type/string, :db.type/ref, :db.type/instant, "
             ":db.type/long, :db.type/bigdec, :db.type/boolean, "
             ":db.type/float, :db.type/uuid, :db.type/double, "
             ":db.type/bigint, :db.type/uri.")}

   cardinality
   {ident :db/cardinality
    value-type type-ref
    cardinality cardinality-one
    doc (str "Property of an attribute. Two possible values: "
             ":db.cardinality/one for single-valued attributes, and "
             ":db.cardinality/many for many-valued attributes. "
             "Defaults to :db.cardinality/one.")}

   unique
   {ident :db/unique
    value-type type-ref
    cardinality cardinality-one
    doc (str "Property of an attribute. If value is :db.unique/value, "
             "then attribute value is unique to each entity. Attempts "
             "to insert a duplicate value for a temporary entity id will "
             "fail. If value is :db.unique/identity, then attribute value "
             "is unique, and upsert is enabled. Attempting to insert a "
             "duplicate value for a temporary entity id will cause all "
             "attributes associated with that temporary id to be merged "
             "with the entity already in the database. Defaults to nil.")}

   index
   {ident :db/index
    value-type type-boolean
    cardinality cardinality-one
    doc (str "Property of an attribute. If true, create an AVET index for "
             "the attribute. Defaults to false.")}

   fulltext
   {ident :db/fulltext
    value-type type-boolean
    doc (str "Property of an attribute. If true, create a fulltext search "
             "index for the attribute. Defaults to false.")}

   is-component
   {ident :db/isComponent
    value-type type-boolean
    doc (str "Property of attribute whose value type is :db.type/ref. "
             "If true, then the attribute is a component of the entity "
             "referencing it. When you query for an entire entity, "
             "components are fetched automatically. Defaults to nil.")}

   no-history
   {ident :db/noHistory
    value-type type-boolean
    doc (str "Property of an attribute. If true, past values of the "
             "attribute are not retained after indexing. Defaults to false.")}

   tx-instant
   {ident :db/txInstant
    value-type type-instant
    index true
    doc (str "Attribute whose value is a :db.type/instant. A :db/txInstant "
             "is recorded automatically with every transaction.")}

   doc
   {ident :db/doc
    value-type type-string
    fulltext true
    doc "Documentation string for an entity."}})

