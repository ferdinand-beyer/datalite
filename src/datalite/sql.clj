(ns datalite.sql
  "JDBC helpers."
  (:import [java.sql Connection]))

(defmacro with-tx
  "Assumes conn to be in auto-commit mode.  Leaves auto-commit
  mode to begin a transaction, and runs the exprs in an implicit
  do.  If exprs succeed without exception, commits conn.
  Otherwise, rolls conn back.  Either way, sets conn back to
  auto-commit mode."
  [conn & exprs]
  (let [conn (vary-meta conn assoc :tag `Connection)]
    `(do
       (.setAutoCommit ~conn false)
       (try
         ~@exprs
         (.commit ~conn)
         (catch Exception e#
           (.rollback ~conn)
           (throw e#))
         (finally
           (.setAutoCommit ~conn true))))))
