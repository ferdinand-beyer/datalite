(ns datalite.util
  (:require [clojure.string :as s]))

(defmacro s
  "Concatenates strings at compile time."
  [& strings]
  (s/join strings))

(defn throw-error
  ([err]
   (throw-error err (str "error " err)))
  ([err msg]
   (throw (ex-info msg {:db/error err})))
  ([err msg info]
   (throw (ex-info msg (assoc info :db/error err))))
  ([err msg info cause]
   (throw (ex-info msg (assoc info :db/error err) cause))))

