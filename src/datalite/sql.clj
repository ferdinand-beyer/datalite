(ns datalite.sql
  "JDBC helpers."
  (:require [clojure.string :as s]
            [clojure.tools.logging :as log])
  (:import [java.sql Connection PreparedStatement
            ResultSet Statement]))

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

(defprotocol Name
  (dbname [this]))

(extend-protocol Name
  String
  (dbname [s] s)

  clojure.lang.Named
  (dbname [n] (name n)))

(defn quoted
  [x]
  (str \" (s/replace (str x) "\"" "\"\"") \"))

(def quoted-name (comp quoted dbname))

(defn- sql-col-vals
  [cols]
  (s/join ", " (map #(str (quoted-name %) " = ?") cols)))

(defn insert-sql
  "Returns a SQL command to INSERT values for cols into table."
  [table cols]
  (let [[n tablecols]
        (if (sequential? cols)
          [(count cols)
           (str " (" (s/join ", " (map quoted-name cols)) ")")]
          [cols nil])]
  (str "INSERT INTO " (quoted-name table) tablecols
       " VALUES (" (s/join ", " (repeat n "?")) ")")))

(defn update-sql
  "Returns a SQL command to UPDATE cols in table with an
  optional where."
  ([table cols]
   (str "UPDATE " (quoted-name table) " SET " (sql-col-vals cols)))
  ([table cols where]
   (str (update-sql table cols) " WHERE " where)))

(defn log-sql
  "Log a SQL statement about to be executed."
  [sql]
  (log/debug sql))

(defn extract
  [^ResultSet rs ^Integer index]
  (.getObject rs index))

(defn set-param
  [^PreparedStatement stmt index value]
  (.setObject stmt index value))

(defn- extract-rows
  [^ResultSet rs indexes]
  (when (.next rs)
    (cons (mapv (partial extract rs) indexes)
          (lazy-seq (extract-rows rs indexes)))))

(defn- result-set
  [^ResultSet rs]
  (let [rsmeta (.getMetaData rs)
        indexes (range 1 (inc (.getColumnCount rsmeta)))]
    (extract-rows rs indexes)))

(defn- set-params
  [stmt params]
  (dorun (map-indexed (fn [i p] (set-param stmt (inc i) p))
                      params)))

(defn- prepared?
  [s]
  (instance? PreparedStatement s))

(defn do-query
  ([^Connection con sql f]
   (if (prepared? sql)
     (with-open [rs (.executeQuery ^PreparedStatement sql)]
       (f (result-set rs)))
     (do
       (log-sql sql)
       (with-open [stmt (.createStatement con)
                   rs (.executeQuery stmt sql)]
         (f (result-set rs))))))
  ([^Connection con sql params f]
   (if (prepared? sql)
     (let [stmt ^PreparedStatement sql]
       (set-params stmt params)
       (with-open [rs (.executeQuery stmt)]
         (f (result-set rs))))
     (do
       (log-sql sql)
       (with-open [stmt (.prepareStatement con sql)]
         (set-params stmt params)
         (with-open [rs (.executeQuery stmt)]
           (f (result-set rs))))))))

(defn exec!
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

(defn query
  ([con sql]
   (do-query con sql doall))
  ([con sql params]
   (do-query con sql params doall)))

(defn query-first
  ([con sql]
   (do-query con sql first))
  ([con sql params]
   (do-query con sql params first)))

(defn query-val
  ([con sql]
   (do-query con sql ffirst))
  ([con sql params]
   (do-query con sql params ffirst)))

(defn cols
  [data]
  (keys data))

(defn values
  [data]
  (vals data))

(defn insert!
  "Executes an INSERT for data into table."
  [con table data]
  (exec! con (insert-sql table (cols data))
         (values data)))

(defn insert-many!
  "Executes an INSERT of cols into table for each
  collection of values in valueseq."
  [con table cols valueseq]
  (exec-many! con (insert-sql table cols) valueseq))

(defn update!
  "Executes an UPDATE for data on table.

  When no params are given, where can also be a map, matching
  rows by key-value pairs."
  ([con table data]
   (exec! con (update-sql table (cols data))
          (values data)))
  ([con table data where]
   (if (map? where)
     (update! con table data
              (sql-col-vals (keys where))
              (vals where))
     (exec! con (update-sql table (cols data) where)
            (values data))))
  ([con table data where params]
   (exec! con (update-sql table (cols data) where)
          (concat (values data) params))))

