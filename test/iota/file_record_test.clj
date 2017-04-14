(ns iota.file-record-test
  (:require [clojure.test :refer :all]
            [clojure.java.io :as jio]
            [clojure.core.reducers :as r]
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
         (trim (last (io/rec-seq tfile 10 sep))))))

(deftest simple-chunk-read

  (is (= 1 (count (io/chunk-seq tfile 1000 sep))))

  (is (= 2 (count (io/chunk-seq tfile 15 sep))))

  (is (= 3 (count (io/chunk-seq tfile 10 sep))))

  (is (= "<a>\nfirst\n</a>"
         (ffirst (io/chunk-seq tfile 15 sep))))

  (is (= "<a>\nlast\n</a>"
         (trim (first (second (io/chunk-seq tfile 15 sep)))))))

(def csv-row "1997,Ford,E350,\"Super, luxurious truck\"\n")

(defn create-csv [num-of-rows]
  (let [file (File/createTempFile "test-csv" ".csv")]
    (with-open [w (jio/writer file)]
      (doseq [_ (range num-of-rows)]
        (.write w csv-row)))
    file))

(deftest huge-chunk-read
  (let [;rows 100000000 ; creates 3.8GB csv file
        rows 10000
        file (create-csv rows)
        path (.getAbsolutePath file)]

    (println "File size:" (/ (.length file) 1024 1024.0) "mb")

    (is (= rows (->> (io/rec-seq path) (r/map (fn [_] 1)) (r/fold +))))

    (println "Num of chunks" (count (io/chunk-seq path 1024)))))