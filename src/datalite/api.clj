(ns datalite.api
  (:refer-clojure :exclude [filter])
  (:require [datalite.connection :as conn]
            [datalite.database :as db]))

; TODO: Decide for each API function whether to support it, and
; give a reason if not/never.
; (Implement throwing UnsupportedOperationException?)
; (Better still using a reasonable (ex-info ...)?)

;(def add-listener)
(def as-of)
(def as-of-t)
(def attribute)
(def basis-t)
(def close conn/close)
(def connect conn/connect)
;(def create-database)
(def datoms)

(defn db
  "Retrieves a value of the database for reading."
  [conn]
  (db/db conn))

;(def delete-database)
(def entid)
(def entid-at)
(def entity)
(def entity-db)
(def filter)
(def function)
;(def gc-storage)
;(def get-database-names)
(def history)
(def ident)
(def index-range)
(def invoke)
(def is-filtered)
(def log)
(def next-t)
(def part)
(def pull)
(def pull-many)
(def q)
(def query)
(def release)
;(def remove-tx-report-queue)
;(def rename-database)
;(def request-index)
(def resolve-tempid)
(def seek-datoms)
;(def shutdown)
(def since)
(def since-t)
;(def squuid)
;(def squuid-time-millis)
;(def sync)
;(def sync-excise)
;(def sync-index)
;(def sync-schema)
(def t->tx)
(def tempid)
(def touch)
(def transact)
(def transact-async)
(def tx->t)
(def tx-range)
;(def tx-report-queue)
(def with)

