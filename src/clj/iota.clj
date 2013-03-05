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


;; COPY of clojure.core/reducer's fork/join pools implementation
;; and foldvec implementation 
;; included here because foldvec needs to call these but they're private...
(defmacro ^:private compile-if
  "Evaluate `exp` and if it returns logical true and doesn't error, expand to
  `then`.  Else expand to `else`.

  (compile-if (Class/forName \"java.util.concurrent.ForkJoinTask\")
    (do-cool-stuff-with-fork-join)
    (fall-back-to-executor-services))"
  [exp then else]
  (if (try (eval exp)
           (catch Throwable _ false))
    `(do ~then)
        `(do ~else)))

(compile-if
 (Class/forName "java.util.concurrent.ForkJoinTask")
 ;; We're running a JDK 7+
 (do
   (def pool (delay (java.util.concurrent.ForkJoinPool.)))

   (defn fjtask [^Callable f]
     (java.util.concurrent.ForkJoinTask/adapt f))

   (defn fjinvoke [f]
     (if (java.util.concurrent.ForkJoinTask/inForkJoinPool)
       (f)
       (.invoke ^java.util.concurrent.ForkJoinPool @pool ^java.util.concurrent.ForkJoinTask (fjtask f))))

   (defn fjfork [task] (.fork ^java.util.concurrent.ForkJoinTask task))

   (defn fjjoin [task] (.join ^java.util.concurrent.ForkJoinTask task)))
 ;; We're running a JDK <7
 (do
   (def pool (delay (jsr166y.ForkJoinPool.)))

   (defn fjtask [^Callable f]
     (jsr166y.ForkJoinTask/adapt f))
   
 (defn fjinvoke [f]
   (if (jsr166y.ForkJoinTask/inForkJoinPool)
     (f)
     (.invoke ^jsr166y.ForkJoinPool @pool ^jsr166y.ForkJoinTask (fjtask f))))

 (defn fjfork [task] (.fork ^jsr166y.ForkJoinTask task))
 
 (defn fjjoin [task] (.join ^jsr166y.ForkJoinTask task))))


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
