(ns datalite.sql
  "JDBC helpers."
  (:require [clojure.core.protocols]
            [clojure.string :as s]
            [clojure.tools.logging :as log])
  (:import [java.sql Connection PreparedStatement
            ResultSet Statement]))

(defn log-sql
  "Log a SQL statement about to be executed."
  [sql]
  (log/debug sql))

(defmacro with-tx
  "Assumes con to be in auto-commit mode.  Leaves auto-commit
  mode to begin a transaction, and runs the exprs in an implicit
  do.  If exprs succeed without exception, commits conn.
  Otherwise, rolls con back.  Either way, sets con back to
  auto-commit mode."
  [con & exprs]
  (let [con (vary-meta con assoc :tag `Connection)]
    `(let [ac# (.getAutoCommit ~con)]
       (.setAutoCommit ~con false)
       (try
         ~@exprs
         (catch Exception e#
           (.rollback ~con)
           (throw e#))
         (finally
           (.setAutoCommit ~con ac#))))))

(defprotocol SQLName
  "Protocol for types that can be used as SQL names."
  (sql-name [this]))

(extend-protocol SQLName
  String
    (sql-name [s] s)
  clojure.lang.Named
    (sql-name [n] (name n)))

(defn quoted
  "Quotes a string so that it can be used as a SQL identifier."
  [x]
  (str \" (s/replace (str x) "\"" "\"\"") \"))

(def quoted-name
  "Produces a quoted SQL name."
  (comp quoted sql-name))

(defn- sql-col-vals
  [sep cols]
  (s/join sep (map #(str (quoted-name %) " = ?") cols)))

(defn sql-insert
  "Returns a SQL command to INSERT values for cols into table."
  [table cols]
  (let [[n tablecols]
        (if (sequential? cols)
          [(count cols)
           (str " (" (s/join ", " (map quoted-name cols)) ")")]
          [cols nil])]
    (str "INSERT INTO " (quoted-name table) tablecols
         " VALUES (" (s/join ", " (repeat n "?")) ")")))

(defn sql-update
  "Returns a SQL command to UPDATE cols in table with an
  optional where."
  ([table cols]
   (str "UPDATE " (quoted-name table) " SET " (sql-col-vals ", " cols)))
  ([table cols where]
   (str (sql-update table cols) " WHERE " where)))

(defn- get-value
  [^ResultSet rs ^Integer index]
  (.getObject rs index))

(defn- set-param
  [^PreparedStatement stmt index value]
  (.setObject stmt index value))

(defn- default-row-fn
  "Return the default row function that will produce vectors."
  [^ResultSet rs]
  (let [rsmeta (.getMetaData rs)
        indexes (range 1 (inc (.getColumnCount rsmeta)))]
    (fn [rs] (mapv (partial get-value rs) indexes))))

(defn- rs-reduce
  "Reduces the ResultSet rs using the reduct function f and
  initial value start.  Uses row-fn to obtain the next row
  from rs."
  [^ResultSet rs f start row-fn]
  (loop [ret start]
    (if (.next rs)
      (let [ret (f ret (row-fn rs))]
        (if (reduced? ret)
          @ret
          (recur ret)))
      ret)))

(extend-protocol clojure.core.protocols/CollReduce
  ResultSet
  (coll-reduce
    ([^ResultSet rs f]
     (let [row-fn (default-row-fn rs)]
       (if (.next rs)
         (rs-reduce rs f (row-fn rs) row-fn)
         (f))))
    ([rs f start]
     (rs-reduce rs f start (default-row-fn rs)))))

(defn- rs-seq
  "Returns a lazy sequence for the ResultSet rs,
  obtaining row values using row-fn."
  ([rs]
   (rs-seq rs (default-row-fn rs)))
  ([^ResultSet rs row-fn]
   (lazy-seq
     (when (.next rs)
       (cons (row-fn rs) (rs-seq rs row-fn))))))

(defn- set-params
  "Sets all params to the prepared stmt using set-param."
  [stmt params]
  (dorun (map-indexed (fn [i p] (set-param stmt (inc i) p))
                      params)))

(defn- prepared?
  "Returns true if s is PreparedStatement."
  [s]
  (instance? PreparedStatement s))

(defn exec!
  "Executes a (non-query) SQL statement."
  ([^Connection con sql]
   (if (prepared? sql)
     (.executeUpdate ^PreparedStatement sql)
     (do
       (log-sql sql)
       (with-open [stmt (.createStatement con)]
         (.executeUpdate stmt sql)))))
  ([^Connection con sql params]
   (if (prepared? sql)
     (let [stmt ^PreparedStatement sql]
       (set-params stmt params)
       (.executeUpdate stmt))
     (do
       (log-sql sql)
       (with-open [stmt (.prepareStatement con sql)]
         (set-params stmt params)
         (.executeUpdate stmt))))))

(defn exec-many!
  "Executes many (non-query) SQL statements, or a single
  statement repeatedly for different parameter vectors."
  ([^Connection con sqlseq]
   (with-open [stmt (.createStatement con)]
    (doseq [sql sqlseq]
      (log-sql sql)
      (.addBatch stmt sql))
    (reduce + (.executeBatch stmt))))
  ([^Connection con sql paramseq]
   (log-sql sql)
   (with-open [stmt (.prepareStatement con sql)]
     (reduce + (map (fn [params]
                      (set-params stmt params)
                      (.executeUpdate stmt))
                    paramseq)))))

(defn run-query
  "Run a query and pass the raw ResultSet to f."
  ([^Connection con sql f]
   (if (prepared? sql)
     (with-open [rs (.executeQuery ^PreparedStatement sql)]
       (f rs))
     (do
       (log-sql sql)
       (with-open [stmt (.createStatement con)
                   rs (.executeQuery stmt sql)]
         (f rs)))))
  ([^Connection con sql params f]
   (if (prepared? sql)
     (let [stmt ^PreparedStatement sql]
       (set-params stmt params)
       (with-open [rs (.executeQuery stmt)]
         (f rs)))
     (do
       (log-sql sql)
       (with-open [stmt (.prepareStatement con sql)]
         (set-params stmt params)
         (with-open [rs (.executeQuery stmt)]
           (f rs)))))))

(defmacro ^:private defquery
  "Define a query function that runs a given query
  and processes the ResultSet."
  [name binding & body]
  {:pre [(vector? binding)
         (= 1 (count binding))]}
  `(defn ~name
     {:arglists '([~'con ~'sql]
                  [~'con ~'sql ~'params])}
     ([con# sql#]
      (run-query con# sql# (fn ~binding ~@body)))
     ([con# sql# params#]
      (run-query con# sql# params# (fn ~binding ~@body)))))

(defquery
  ^{:doc "Runs query sql and returns a sequence of vectors."}
  query [rs]
  (doall (rs-seq rs)))

(defquery
  ^{:doc "Runs query sql and returns the first row as a vector."}
  query-first [rs]
  (first (rs-seq rs)))

(defquery
  ^{:doc "Runs query sql and returns the first column
    of the first row."}
  query-val [rs]
  (ffirst (rs-seq rs)))

(defn- cols
  "Extracts columns from UPSERT data."
  [data]
  (keys data))

(defn- values
  "Extracts values from UPSERT data."
  [data]
  (vals data))

(defn insert!
  "Executes an INSERT for data into table."
  [con table data]
  (exec! con (sql-insert table (cols data))
         (values data)))

(defn insert-many!
  "Executes an INSERT of cols into table for each
  collection of values in valueseq."
  [con table cols valueseq]
  (exec-many! con (sql-insert table cols) valueseq))

(defn update!
  "Executes an UPDATE for data on table.

  When no params are given, where can also be a map, matching
  rows by key-value pairs."
  ([con table data]
   (exec! con (sql-update table (cols data))
          (values data)))
  ([con table data where]
   (if (map? where)
     (update! con table data
              (sql-col-vals " AND " (keys where))
              (vals where))
     (exec! con (sql-update table (cols data) where)
            (values data))))
  ([con table data where params]
   (exec! con (sql-update table (cols data) where)
          (concat (values data) params))))

