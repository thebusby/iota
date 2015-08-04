(ns iota.file-record-test
  (:require [clojure.test :refer :all]
            [clojure.core.reducers :as r]
            [clojure.java.io :as jio]
            [clojure.string :refer [trim]]
            [iota :as io])
  (:import (java.io File)))


(defn res [path] (jio/resource path))
(defn abs-path [res] (.getAbsolutePath (File. (.toURI res))))

(def tfile (abs-path (res "iota/record-test.txt")))

(def sep (map int "</a>"))

(deftest simple-read

  (is (= "<a>\nfirst\n</a>"
         (trim (first (io/rec-seq tfile 10 sep)))))

  (is (= "<a>\nlast\n</a>"
         (trim (last (io/rec-seq tfile 10 sep)))))

  )
