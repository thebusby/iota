(defproject iota "1.1.1"
  :description        "Allows Clojure's reducers to operate over mmap()'ed text files"
  :url                "https://github.com/thebusby/iota"
  :license            {:name "Eclipse Public License"
                       :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies       [[org.clojure/clojure "1.5.1"]
                       [org.codehaus.jsr166-mirror/jsr166y "1.7.0"] ;; To support reducers on Java 1.6
                       ]
  :source-paths       ["src/clj"]
  :java-source-paths  ["src/java"]
  :source-path        "src/clj"
  :java-source-path   "src/java")
