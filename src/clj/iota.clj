(ns iota
  (:require [clojure.core.reducers :as r])
  (:import (iota FileVector NumberedFileVector))
  (:refer-clojure :exclude [vec subvec]))
(set! *warn-on-reflection* true)


;; ;;; ;; ;;; ;; ;;; ;; ;;; ;; ;;; ;; ;;; ;; ;;; ;; ;;; ;; ;;;
;; Public functions
;; ;;; ;; ;;; ;; ;;; ;; ;;; ;; ;;; ;; ;;; ;; ;;; ;; ;;; ;; ;;;

(defn ^iota.FileVector vec
  "Return a vector like structure mmap'd over a flatfile on disk.
   You can provide the chunk size and a single char field delimiter as well."  
  ([^java.lang.String filename] (FileVector. filename))
  ([^java.lang.String filename chunk-size] (new iota.FileVector filename (int chunk-size))))

(defn subvec
  "Return a subset of the provided flatfileclj vector.
   If end not provided, defaults to (count v)."
  ([^iota.FileVector v start]     (subvec  v start (count v)))
  ([^iota.FileVector v start end] (.subvec v start end)))

(defn numbered-vec
  "Return a NumberedFileVector, which has the line number appended to the
   beginning of each line with the provided delimiter (default \tab)"
  ([^java.lang.String filename] (numbered-vec filename 10))
  ([^java.lang.String filename chunk-size] (numbered-vec filename chunk-size "\t"))
  ([^java.lang.String filename chunk-size delim] (new iota.NumberedFileVector filename (int chunk-size) delim)))


;; ;;; ;; ;;; ;; ;;; ;; ;;; ;; ;;; ;; ;;; ;; ;;; ;; ;;; ;; ;;;
;; Code to enable reducers;
;; ;;; ;; ;;; ;; ;;; ;; ;;; ;; ;;; ;; ;;; ;; ;;; ;; ;;; ;; ;;;

;; Bind to core.reducer's private ForkJoin functions
(def fjinvoke #'r/fjinvoke)
(def fjfork #'r/fjfork)
(def fjjoin #'r/fjjoin)


;; Implement CollFold for FileVector
;; Note: copied+modified from clojure.core.reducers/foldvec
(defn- foldvec
  "Utility function to enable reducers for FlatFileCLJ Vector's"
  [^iota.FileVector v n combinef reducef]
  (cond
   (empty? v) (combinef)
   (<= (count v) n) (reduce reducef (combinef) v)
   :else    
   (let [split (quot (count v) 2)
         v1 (.subvec v 0 split)
         v2 (.subvec v split (count v))
         fc (fn [child] #(foldvec child n combinef reducef))]
     (fjinvoke
      #(let [f1 (fc v1)
             t2 (r/fjtask (fc v2))]
         (fjfork t2)
         (combinef (f1) (fjjoin t2)))))))

(extend-protocol r/CollFold
  iota.FileVector
  (coll-fold
    [v n combinef reducef]
    (foldvec v n combinef reducef)))
