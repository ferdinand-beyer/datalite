# Transactions

The `transact` function is implemented as a process of transducers,
transforming the `tx-data` sequence into transaction operations, and
finally emitting SQL and producing a transaction report.

## Transaction context

At first, the latest state of the database is captured in a transaction
context.  This is a map that contains:

* `:db` - the latest database value that will become `:db-before` in
  the transaction report.
* `:t` - the t value of the transaction, which is the `next-t` value of
  `:db`.
* `:tx` - the id of the transaction entity, derived from `:t`.
* `:instant` - the wall clock time at the start of the transaction
  processing
* `:tempids` - a map of temporary ids to entity ids

## Transaction operations

All forms in the `tx-data` sequence are analyzed and reduced to base
operations.  These "ops" are simple maps containing:

* `:op` - one of `:add` or `:retract`
* `:form` - the original form
* `:e` - entity identifier
* `:a` - attribute identifier
* `:v` - value to add or retract

## Transaction report

The transaction report is the return value of `transact`.  It is a map
that contains:

* `:db-before` - the database value before the transaction
* `:db-after` - the database value after the transaction
* `:tx-data` - the datoms `[:e :a :v :tx :added]` produced by the
  transaction.  Note that this is different from the input `tx-data`.
* `tempids` - a map from temporary ids to resolved entity ids.

## Phases

Transaction processing involves multiples phases.

First, we transform a `tx-data` collection into transaction ops.  On the
way, we infer as much information as possible by analyzing individual
ops.

* `analyze-form`
* `reverse-attr`
* `resolve-attr`
* `resolve-entity` - existing only
* `protect-system`
* `exand-multival`
* `expand-nested`
* `assoc-tx`
* `check-tx-instant`
* `check-unique`

Afterwards, we do cross-operation analysis, filling in the missing
pieces:

* Assign new entity ids to tempids
* Check schema attributes
* Determine tx instant
* Generate additional operations (e.g. `:db.install`)

With all information together, we do a second pass over the operations:

* `new-entity`
* `resolve-ref` - no more tempids after this point
* `coerce-value`
* `auto-retract`
* `drop-duplicate`
* `drop-redundant`

Now we have a final collection of operations.

* `exec-sql`
* `tx-report`

## Transducers

Transducers are an elegant and efficient way to implement the multiple
transformation steps required to analyze and process `tx-data`.  Each
transducer returns a reduct function that operates on the _transaction
context_.

### analyze-form

Transforms forms to _transaction operation_ maps.  A transaction
operation captures all information we have about an operation at a given
point in time. The maps will contain:

* `:op` - one of `:add` or `:retract`
* `:form` - the original form
* `:e`
* `:a`
* `:v`

The transformation is involves the following substeps:

* Map forms are expanded to `:add` operations, with tempids for `:e`
  unless a `:db/id` key is given.
* Database functions are invoked recursively until all returned forms
  are reduced to `:add` and `:retract`.

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

Associate all tempids with new entity ids.  Fail if a tempid is used in
a `:retract` operation.

### check-tx-instant

If `:db/txInstant` is asserted on the transaction entity, check that the
value lies between the `:db/txInstant` of the last transaction and the
`:instant` value of the transaction context.  If none is asserted,
create one.

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

### exec-sql

For a `:add` op, execute a `INSERT` query.  For a `:retract` op,
`UPDATE` the `tr` field of the `:data-id` row to the `:t` value of the
transaction context.

### TODO

Additional processing steps to define transducers for:

* Generate missing install datoms for attributes
* Check required schema entities: ident, valueType, cardinality
* Handle schema alterations
* Prevent unique byte attributes
* Return a transaction report
* Prevent :db/add and :db/retract in same tx?
* Update sequence numbers

