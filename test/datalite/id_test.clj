(ns datalite.id-test
  (:require [clojure.test :refer :all]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :refer [for-all]]
            [datalite.id :refer :all]))

(defspec id-with-zero-partition-and-positive-t-is-t-value
  100
  (for-all [t gen/pos-int]
    (= t (eid 0 t))))

(defspec id-with-negative-t-is-negative
  100
  (for-all [p gen/pos-int
            t gen/s-neg-int]
    (neg? (eid p t))))

(defspec extract-partition
  100
  (for-all [p gen/pos-int
            t gen/int]
    (= p (eid->part (eid p t)))))

(defspec extract-t
  100
  (for-all [p gen/pos-int
            t gen/int]
    (= t (eid->t (eid p t)))))

