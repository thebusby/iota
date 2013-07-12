(ns iota.iota-test
  (:use clojure.test)
  (:require [iota :as io]
            [clojure.core.reducers :as r]))


;; Generate some fake data to test with
(def test-dims [3768 13])
(def test-data (let [[rows columns] test-dims]
                 (->> (range rows)
                      (mapv  (comp vec #(range % (+ % columns))))                      
                      vec
                      (#(conj % [0] nil nil nil [42]))))) ;; Add some trailing garbage to make it interesting
(def ^:dynamic test-vec  nil) ;; Will bind an iota/vec here
(def ^:dynamic test-seq  nil) ;; Will bind an iota/vec here
(def ^:dynamic test-nvec nil) ;; Will bind an numbered-vec here

(defn serialize-rec [rec]
  "Convert a test rec into a String"
  (->> rec
       (interpose "\t")
       doall
       (apply str)))

(defn deserialize-rec [^String line]
  "Convert a string back into a test rec"
  (->> (clojure.string/split line #"[\t]" -1)
       (keep (fn [^String x]
               (if (.isEmpty x) 
                 nil 
                 (Long/parseLong x))))
       vec))

(defn- get-rand-subvecs [clj-vec io-vec]
  "Generate random subvectors for each vector provided."
  (let [[rows _] test-dims
        start (rand-int (dec rows))
        end   (+ start 
                 (rand-int (- rows 
                              start)))]
    [(subvec clj-vec start end)
     (io/subvec io-vec start end)]))

(defn fixture-tsv [f]
  "Setup a file full of test-data in system's temp dir
   and create test-vec and test-nvec pointing to it."
  (let [test-file (str (System/getProperty "java.io.tmpdir")
                       (System/getProperty "file.separator")
                       (java.util.UUID/randomUUID)
                       ".tsv")]

    ;; Create Test File
    (println "Fixture: Creating test file of" test-dims "at" test-file)
    (spit test-file
          (->> test-data
               (map serialize-rec)
               (interpose "\n")
               doall
               (apply str)))

    ;; Load iota vec's and run tests
    (binding [test-seq  (io/seq test-file)
              test-vec  (io/vec test-file)
              test-nvec (io/numbered-vec test-file)]
      (f))

    ;; Cleanup
    (println "Fixture: removing file " test-file)
    (-> test-file
        (java.io.File.)
        .delete)))

;; Use fixture for loading/testing
(use-fixtures :once fixture-tsv)


;; -- - -- - -- - -- - -- - -- - -- - -- - -- - -- - -- - 
;;                 Test follow below
;; -- - -- - -- - -- - -- - -- - -- - -- - -- - -- - -- - 
(deftest test-count
  (is (= (count test-data)
         (count test-vec))))

(deftest test-first
  (is (= (first test-data)
         (-> test-vec
             first
             deserialize-rec))))

(deftest test-last
  (is (= (last test-data)
         (-> test-vec
             last
             deserialize-rec))))

(deftest test-scount
  (is (= (count test-data)
         (count test-seq))))

(deftest test-sfirst
  (is (= (first test-data)
         (-> test-seq
             first
             deserialize-rec))))

(deftest test-slast
  (is (= (last test-data)
         (-> test-seq
             last
             deserialize-rec))))

(deftest test-nth
  (doseq [n (range (first test-dims))]
    (is (= (nth test-data n)
           (-> test-vec
               (nth n)
               deserialize-rec)))))

(deftest test-subvec-count
  (dotimes [n 1000]
    (let [[clj-vec io-vec] (get-rand-subvecs test-data test-vec)]
      (is (= (count clj-vec)
             (count io-vec))))))

(deftest test-ncount
  (is (= (count test-data)
         (count test-nvec))))

(deftest test-nfirst
  (is (= (first test-data)
         (-> test-nvec
             first
             deserialize-rec
             rest
             vec))))

(deftest test-nlast
  (is (= (last test-data)
         (-> test-nvec
             last
             deserialize-rec
             rest
             vec))))

(deftest test-nnth
  (doseq [n (range (first test-dims))]
    (is (= (nth test-data n)
           (-> test-nvec
               (nth n)
               deserialize-rec
               rest
               vec)))))

(deftest test-nvec-nth
  (doseq [n (range (first test-dims))]
    (is (= n
           (-> test-nvec
               (nth n)
               deserialize-rec
               first)))))

(deftest test-total-sum
  (are [my-f my-m my-r] (is (= (->> test-data
                                    (my-f identity)
                                    (my-m (partial reduce +))
                                    (my-r +))
                               (->> test-vec
                                    (my-f identity)
                                    (my-m deserialize-rec)
                                    (my-m (partial reduce +))
                                    (my-r +))))
       filter map reduce
       r/filter r/map r/reduce
       r/filter r/map r/fold))

(deftest test-total-sum-n
  (are [my-f my-m my-r] (is (= (->> test-data
                                    (my-f identity)
                                    (my-m (partial reduce +))
                                    (my-r +))
                               (->> test-nvec
                                    (my-f identity)
                                    (my-m (comp rest deserialize-rec))
                                    (my-m (partial reduce +))
                                    (my-r +))))
       filter map reduce
       r/filter r/map r/reduce
       r/filter r/map r/fold))

(deftest test-total-sum-s
  (are [my-f my-m my-r] (is (= (->> test-data
                                    (my-f identity)
                                    (my-m (partial reduce +))
                                    (my-r +))
                               (->> test-seq
                                    (my-f identity)
                                    (my-m deserialize-rec)
                                    (my-m (partial reduce +))
                                    (my-r +))))
       filter map reduce
       r/filter r/map r/reduce
       r/filter r/map r/fold))

(deftest test-total-sub-subvec
  (dotimes [n 1000]
    (let [[clj-vec io-vec] (get-rand-subvecs test-data test-vec)]
      (are [my-f my-m my-r] (is (= (->> clj-vec
                                        (my-f identity)
                                        (my-m (partial reduce +))
                                        (my-r +))
                                   (->> io-vec
                                        (my-f identity)
                                        (my-m deserialize-rec)
                                        (my-m (partial reduce +))
                                        (my-r +))))
           filter map reduce
           r/filter r/map r/reduce
           r/filter r/map r/fold))))

(deftest test-total-sub-subvec-n
  (dotimes [n 1000]
    (let [[clj-vec io-vec] (get-rand-subvecs test-data test-nvec)]
      (are [my-f my-m my-r] (is (= (->> clj-vec
                                        (my-f identity)
                                        (my-m (partial reduce +))
                                        (my-r +))
                                   (->> io-vec
                                        (my-f identity)
                                        (my-m (comp rest deserialize-rec))
                                        (my-m (partial reduce +))
                                        (my-r +))))
           filter map reduce
           r/filter r/map r/reduce
           r/filter r/map r/fold))))
