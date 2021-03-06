(defproject datalite "0.1.0-SNAPSHOT"
  :description "Datomic-inspired database on top of SQLite"
  :url "https://github.com/ferdinand-beyer/datalite"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/tools.logging "0.3.1"]
                 [org.slf4j/slf4j-api "1.7.22"]
                 [org.xerial/sqlite-jdbc "3.16.1"]]
  :test-selectors {:default (complement :integration)
                   :integration :integration}
  :profiles {:dev {:source-paths ["dev"]
                   :dependencies [[ch.qos.logback/logback-classic "1.1.9"]
                                  [org.clojure/test.check "0.9.0"]
                                  [org.clojure/tools.namespace "0.2.11"]]}})

