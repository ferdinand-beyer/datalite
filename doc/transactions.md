# Transactions

The `transact` function is implemented as a process of transducers,
transforming the `tx-data` sequence into transaction operations, with
finally emit SQL and a transaction report.

## Transaction context

(Environment?)

At first, the latest state of the database is captured in a transaction
context.  This is a map that contains:

* `:db` -- the latest database value that will become `:db-before` in
  the transaction report.
* `:t` -- the t value of the transaction, which is the `next-t` value of
  `:db`.
* `:tx` -- the id of the transaction entity, derived from `:t`.
* `:tempids` -- a map of temporary ids to entity ids

## Transaction report

The transaction report is the return value of `transact`.  It is a map
that contains:

* `:db-before` -- the database value before the transaction
* `:db-after` -- the database value after the transaction
* `:tx-data` -- the datoms `[:e :a :v :tx :added]` produced by the
  transaction.  Note that this is different from the input `tx-data`.
* `tempids` -- a map from temporary ids to resolved entity ids.

## Transducers

Transducers are an elegant and efficient way to implement the multiple
transformation steps required to analyze and process `tx-data`.  Each
transducer returns a reduct function that operates on the _transaction
context_.

### op

Transforms forms to _transaction operation_ maps.  A transaction
operation captures all information we have about an operation at a given
point in time.  Initially, this contains:

* `:op` -- one of `:add`, `:retract`, `:add-map`, or `:fn`.
* `:form` -- the original form

### invoke-fn

Acts on `:fn` operations.  Recursively resolves and invokes database
functions, passing the `:db` of the transaction context, until no more
`:fn` operations remain.

### expand-map

Expands `:add-map` operations to `:add` operations, assigning tempids to
them.

### atomic-op

Only accepts `:add` and `:retract` ops with a `:form` that matches
`[:op :e :a :v]`.  Extracts the `:e`, `:a` and `:v` keys into the
operation map.

### reverse-attr

For "reverse attributes" that are keywords with names starting with an
underscore, reverse the operation: `[:e :ns/_ref :v]` becomes `[:v
:ns/ref :e]`.

### resolve-attr

Resolves the `:a` value of the op to an entity id, and makes sure that
it actually is a installed schema attribute.  Stores the attribute
properties from the schema as `:attr` in op, so that subsequent steps
can analyze ops based on the schema.

### resolve-entity

Resolves the `:e` value of the op to an entity id, unless it is a
tempid.

### protect-system

Checks that the operation does not target a system entity.

### expand-multival

If the `:attr` has a cardinality "many", and the `:v` is a collection,
expand the operation into multiple single operations.

### resolve-ref

If the `:attr` has value-type "ref", resolve the `:v` to an entity id.

### expand-nested

If the `:attr` has value-type "ref", and the `:v` is a map, expand the
nested map form.

### coerce-value

Coerces the `:v` value to the SQL type depending on the attribute's
value type, storing `:write-val` in op.

### check-unique

If the `:attr` is unique and `:e` is a tempid, look for an existing
datum `[:a :v]`.  If it exists, "upsert" or fail depending on the
attributes unique type ("identity" or "value").  For upsert, associate
the tempid with the existing entity id.

### assoc-tx

Associate all tempids for the "tx" partition, as well as the reserved
strings "db.tx" and "datomic.tx" (for compatibility), with the `:tx`
from the transaction context.

### assoc-tempid

Associate all tempids with new entity ids.

### check-existing

For `:e` values that are not temporary, check if a `[:e :a :v]` exists
in the database.  If so, store the existing data id as `:data-id` in the
op.  Otherwise, if `:op` is `:retract`, fail.

### auto-retract

If the op is `:add`, the `:e` is not temporary, no `:data-id` exists,
and the cardinality of `:attr` is "one", check if a `[:e :a]` exists in
the database, and if so, create a `:retract` operation for the existing
datum, storing its `:data-id`.

### drop-duplicate

Filter out all duplicate operations.

### drop-redundant

Filter out all `:add` ops with a `:data-id`, as they are redundant.

### TODO

Additional processing steps to define transducers for:

* Check any supplied value for :db/txInstant (newer than every existing
  value, older than current time)
* Generate missing install datoms for attributes
* Check required schema entities: ident, valueType, cardinality
* Handle schema alterations
* Prevent unique byte attributes
* Issue SQL and COMMIT
* Return a transaction report
* Prevent :db/add and :db/retract in same tx?

## Transducer switches

Some steps are not required when a previous step can give certain
guarantees.  For example, `expand-map` guarantees to provide valid
"atomic" ops that don't need to be passed through `atomic-op` any more.

This is implemented with "switches", transducers that take a predicate
and two transducers, routing ops to either one depending on the
predicate.

