(ns datalite.util)

(defmacro s
  "Concatenates strings at compile time."
  [& strings]
  (apply str strings))

