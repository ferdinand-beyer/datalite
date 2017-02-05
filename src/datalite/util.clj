(ns datalite.util)

(defmacro s
  "Concatenates strings at compile time."
  [& strings]
  (apply str strings))

(defn throw-error
  ([err]
   (throw-error err (str "error " err)))
  ([err msg]
   (throw (ex-info msg {:db/error err})))
  ([err msg info]
   (throw (ex-info msg (assoc info :db/error err))))
  ([err msg info cause]
   (throw (ex-info msg (assoc info :db/error err) cause))))

