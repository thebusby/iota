(ns iota
  "A set of tools for using reducers over potentially very large text files."
  (:require [clojure.core.reducers :as r])
  (:import (iota FileVector NumberedFileVector FileSeq FileRecordSeq))
  (:refer-clojure :exclude [vec subvec seq]))

(set! *warn-on-reflection* true)


;; ;;; ;; ;;; ;; ;;; ;; ;;; ;; ;;; ;; ;;; ;; ;;; ;; ;;; ;; ;;;
;; Public functions
;; ;;; ;; ;;; ;; ;;; ;; ;;; ;; ;;; ;; ;;; ;; ;;; ;; ;;; ;; ;;;

(defn ^iota.FileSeq seq
  "Return a seq like structure over an mmap'd file on disk. Poor performance
   for typical ISeq access (first, next, etc), but fast when reduced over.

   You can provide a buffer size in *bytes*, which indicates the buffer size
   to read from disk from, as well as the smallest set of data to fork.
   A byte can be provided to indicate separation between records.

   Default values are a 256KB buffer, and separation on 10 (Newline in ASCII)."
  ([^java.lang.String filename] (FileSeq. filename))
  ([^java.lang.String filename buffer-size] (FileSeq. filename (int buffer-size)))
  ([^java.lang.String filename buffer-size byte-separator] (FileSeq. filename (int buffer-size) (byte byte-separator))))

(defn ^iota.FileVector vec
  "Return a vector like structure mmap'd over a file on disk.

   On creation, an index of the file will be constructed so random access will be O(1),
   similar to a normal Clojure vector. This is significantly more memory effecient than
   a vector of Strings.

   You can provide the chunk size and a single char field delimiter as well."  
  ([^java.lang.String filename] (FileVector. filename))
  ([^java.lang.String filename chunk-size] (new iota.FileVector filename (int chunk-size)))
  ([^java.lang.String filename chunk-size byte-separator] (new iota.FileVector filename (int chunk-size) (byte byte-separator))))

(defn ^iota.FileRecordSeq rec-seq
  "Almost same as FileSeq but record separator can be multibyte array and
   it will *not* strip newlines or separators from output strings."
  ([^java.lang.String filename] (FileRecordSeq. filename))
  ([^java.lang.String filename buffer-size] (FileRecordSeq. filename (int buffer-size)))
  ([^java.lang.String filename buffer-size separator]
   (FileRecordSeq. filename (int buffer-size) (if (sequential? separator)
                                                (byte-array (map byte separator))
                                                (byte-array [(byte separator)])))))


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
  "Utility function to enable reducers for Itoa Vector's"
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

(defn- foldseq
  "Utility function to enable reducers for Iota Seq's"
  [^iota.FileSeq s n combinef reducef]
  (if-let [[v1 v2] (.split s)]
    (let [fc (fn [child] #(foldseq child n combinef reducef))]
      (fjinvoke
       #(let [f1 (fc v1)
              t2 (r/fjtask (fc v2))]
         (fjfork t2)
         (combinef (f1) (fjjoin t2)))))
    (reduce reducef (combinef) (.toArray s))))

(defn- foldrecseq
  "Utility function to enable reducers for Iota RecordSeq's"
  [^iota.FileRecordSeq s n combinef reducef]
  (if-let [[v1 v2] (.split s)]
    (let [fc (fn [child] #(foldrecseq child n combinef reducef))]
      (fjinvoke
        #(let [f1 (fc v1)
               t2 (r/fjtask (fc v2))]
          (fjfork t2)
          (combinef (f1) (fjjoin t2)))))
    (reduce reducef (combinef) (.toArray s))))

(extend-protocol r/CollFold
  iota.FileVector
  (coll-fold
    [v n combinef reducef]
    (foldvec v n combinef reducef))
  iota.FileSeq
  (coll-fold
    [v n combinef reducef]
    (foldseq v n combinef reducef))
  iota.FileRecordSeq
  (coll-fold
    [v n combinef reducef]
    (foldrecseq v n combinef reducef)))