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
                      (#(conj % [0] nil nil nil [42])))))
(def ^:dynamic test-vec nil)
(def ^:dynamic test-nvec nil)


(defn parse-line [^String line]
  "Convert a line of the TSV file back into a vec of longs"
  (->> (clojure.string/split line #"[\t]" -1)
       (keep (fn [^String x]
               (if (.isEmpty x) 
                 nil 
                 (Long/parseLong x))))
       vec))

(defn fixture-tsv [f]
  "Setup a file full of test-data in system's temp dir
   and create test-vec and test-nvec pointing to it."
  (let [test-file (str (System/getProperty "java.io.tmpdir")
                       (java.util.UUID/randomUUID)
                       ".tsv")]

    ;; Create Test File
    (println "Fixture: Creating test file of" test-dims "at" test-file)
    (spit test-file
          (->> test-data
               (map (comp (partial apply str) doall (partial interpose "\t")))
               (interpose "\n")
               doall
               (apply str)))

    ;; Load iota vec's and run tests
    (binding [test-vec  (io/vec test-file)
              test-nvec (io/numbered-vec test-file)]
     (f))

    ;; Cleanup
    (println "Fixture: removing file " test-file)
    (-> test-file
        (java.io.File.)
        .delete)))


;; Use fixture for loading/testing
(use-fixtures :once fixture-tsv)


(deftest test-first
  (is (= (first test-data)
         (-> test-vec
             first
             parse-line))))

(deftest test-last
  (is (= (last test-data)
         (-> test-vec
             last
             parse-line))))

(deftest test-nth
  (are [n] (= (nth test-data n)
              (-> test-vec
                  (nth n)
                  parse-line))
        0
        1
        2
        3
        5
        10
        50
        100
        250
        500
        999))

(deftest test-count
  (is (= (count test-data)
         (count test-vec))))

(deftest test-subvec-count
  (is (= (count (subvec test-data
                        (* 0.3 (first test-dims))
                        (* 0.7 (first test-dims))))
         (count (io/subvec test-vec
                         (* 0.3 (first test-dims))
                         (* 0.7 (first test-dims)))))))

(deftest test-ncount
  (is (= (count test-data)
         (count test-nvec))))

(deftest test-nfirst
  (is (= (first test-data)
         (-> test-nvec
             first
             parse-line
             rest
             vec))))

(deftest test-nlast
  (is (= (last test-data)
         (-> test-nvec
             last
             parse-line
             rest
             vec))))

(deftest test-nnth
  (are [n] (= (nth test-data n)
              (-> test-nvec
                  (nth n)
                  parse-line
                  rest
                  vec))
        0
        1
        2
        3
        5
        10
        50
        100
        250
        500
        999))

(deftest test-nvec-nth
  (are [n] (= n
              (-> test-nvec
                  (nth n)
                  parse-line
                  first))
        0
        1
        2
        3
        5
        10
        50
        100
        250
        500
        999))


(deftest test-total-sum
  (is (= (->> test-data
              (filter identity)
              (map (partial reduce +))
              (reduce +))
         (->> test-vec
              (filter identity)
              (map parse-line)
              (map (partial reduce +))
              (reduce +)))))

(deftest test-total-sum-with-reducers
  (is (= (->> test-data
              (r/filter identity)
              (r/map (partial reduce +))
              (r/reduce +))
         (->> test-vec
              (r/filter identity)
              (r/map parse-line)
              (r/map (partial reduce +))
              (r/reduce +)))))

(deftest test-total-sum-with-fold
  (is (= (->> test-data
              (r/filter identity)
              (r/map (partial reduce +))
              (r/fold +))
         (->> test-vec
              (r/filter identity)
              (r/map parse-line)
              (r/map (partial reduce +))
              (r/fold +)))))

(deftest test-total-sum-n
  (is (= (->> test-data
              (filter identity)
              (map (partial reduce +))
              (reduce +))
         (->> test-nvec
              (filter identity)
              (map (comp rest parse-line))
              (map (partial reduce +))
              (reduce +)))))

(deftest test-total-sum-with-reducers-n
  (is (= (->> test-data
              (r/filter identity)
              (r/map (partial reduce +))
              (r/reduce +))
         (->> test-nvec
              (r/filter identity)
              (r/map (comp rest parse-line))
              (r/map (partial reduce +))
              (r/reduce +)))))

(deftest test-total-sum-with-fold-n
  (is (= (->> test-data
              (r/filter identity)
              (r/map (partial reduce +))
              (r/fold +))
         (->> test-nvec
              (r/filter identity)
              (r/map (comp rest parse-line))
              (r/map (partial reduce +))
              (r/fold +)))))

(deftest test-total-sum-subvec
  (is (= (->> (subvec test-data
                      (* 0.3 (first test-dims))
                      (* 0.7 (first test-dims)))
              (filter identity)
              (map (partial reduce +))
              (reduce +))
         (->> (io/subvec test-vec
                         (* 0.3 (first test-dims))
                         (* 0.7 (first test-dims)))
              (filter identity)
              (map parse-line)
              (map (partial reduce +))
              (reduce +)))))

(deftest test-total-sum-with-reducers-subvec
  (is (= (->> (subvec test-data
                      (* 0.3 (first test-dims))
                      (* 0.7 (first test-dims)))
              (r/filter identity)
              (r/map (partial reduce +))
              (r/reduce +))
         (->> (io/subvec test-vec
                         (* 0.3 (first test-dims))
                         (* 0.7 (first test-dims)))
              (r/filter identity)
              (r/map parse-line)
              (r/map (partial reduce +))
              (r/reduce +)))))

(deftest test-total-sum-with-fold-subvec
  (is (= (->> (subvec test-data
                      (* 0.3 (first test-dims))
                      (* 0.7 (first test-dims)))
              (r/filter identity)
              (r/map (partial reduce +))
              (r/fold +))
         (->> (io/subvec test-vec
                         (* 0.3 (first test-dims))
                         (* 0.7 (first test-dims)))
              (r/filter identity)
              (r/map parse-line)
              (r/map (partial reduce +))
              (r/fold +)))))

(deftest test-total-sum-n-subvec
  (is (= (->> (subvec test-data
                      (* 0.3 (first test-dims))
                      (* 0.7 (first test-dims)))
              (filter identity)
              (map (partial reduce +))
              (reduce +))
         (->> (io/subvec test-nvec
                         (* 0.3 (first test-dims))
                         (* 0.7 (first test-dims)))
              (filter identity)
              (map (comp rest parse-line))
              (map (partial reduce +))
              (reduce +)))))

(deftest test-total-sum-with-reducers-n-subvec
  (is (= (->> (subvec test-data
                      (* 0.3 (first test-dims))
                      (* 0.7 (first test-dims)))
              (r/filter identity)
              (r/map (partial reduce +))
              (r/reduce +))
         (->> (io/subvec test-nvec
                         (* 0.3 (first test-dims))
                         (* 0.7 (first test-dims)))
              (r/filter identity)
              (r/map (comp rest parse-line))
              (r/map (partial reduce +))
              (r/reduce +)))))

(deftest test-total-sum-with-fold-n-subvec
  (is (= (->> (subvec test-data
                      (* 0.3 (first test-dims))
                      (* 0.7 (first test-dims)))
              (r/filter identity)
              (r/map (partial reduce +))
              (r/fold +))
         (->> (io/subvec test-nvec
                         (* 0.3 (first test-dims))
                         (* 0.7 (first test-dims)))
              (r/filter identity)
              (r/map (comp rest parse-line))
              (r/map (partial reduce +))
              (r/fold +)))))
