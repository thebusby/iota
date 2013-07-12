(ns iota
  "A set of tools for using reducers over potentially very large text files."
  (:require [clojure.core.reducers :as r])
  (:import (iota FileVector NumberedFileVector FileSeq))
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

(extend-protocol r/CollFold
  iota.FileVector
  (coll-fold
    [v n combinef reducef]
    (foldvec v n combinef reducef))
  iota.FileSeq
  (coll-fold
    [v n combinef reducef]
    (foldseq v n combinef reducef)))





(defn fold-into-vec
  "Provided a reducer, concatenate into a vector.
   Note: same as (into [] coll), but parallel."
  ([coll]   (r/fold   (r/monoid into vector) conj coll))
  ([n coll] (r/fold n (r/monoid into vector) conj coll)))

(comment
;; start of comments




(time (let [foo (FileSeq. "/tmp/album.tsv" (int 10) (byte 10))] 
        (->> foo 
             rest 
             (r/map (fn [^String s] (.split s "[\\t]" -1))) 
             (r/map first)  
             fold-into-vec ((juxt count #(take 10 %))))))


(time (let [foo (FileSeq. "/tmp/album.tsv" (int 10) (byte 10))] 
        (->> foo 
             rest 
             (r/map (fn [^String s] (.split s "[\\t]" -1))) 
             (r/map first)  
             fold-into-vec ((juxt count last)))))


(def foo (FileSeq. "/tmp/album.tsv" (int 10) (byte 10)))

(.mapchr foo 763 7790 (byte 10) -3)

(defn mget [foo start end]
  (let [len (- end start)
        buf (byte-array len)
        ^iota.Mmap map (.map foo)]
    (.get map buf start (int len))
    (java.lang.String. buf (int 0) (int len) "UTF-8")))

(.get mmap ba 0 (dec (count ba)))



(.mapchr foo 763 7790 (byte 10) -3) 
;; 1109


(import [iota Mmap])

(def album-filename "/data/tsvs/album.tsv")

(def fs (FileSeq. "/tmp/data.txt"))
(def fs2 (FileSeq. "/tmp/data.txt"))
(def fs3 (FileSeq. "/tmp/data.txt" (int 100) (byte 10)))


(def foo (FileSeq. "/tmp/album.tsv" (int 100) (byte 10)))




(time (->> foo 
           rest 
           (map (fn [^String s] (.split s "[\\t]" -1))) 
           (map (comp #(Long/parseLong %) first))  
           (reduce +)))

(time (let [foo (FileSeq. "/tmp/album2.tsv")] 
        (->> foo 
             rest 
             (r/map (fn [^String s] (.split s "[\\t]" -1))) 
             (r/map (comp #(Long/parseLong %) first))  
             (r/fold +))))

(time (let [foo (FileSeq. "/tmp/album2.tsv" (int 100000) (byte 10))] 
        (->> foo 
             rest 
             (r/map (fn [^String s] (.split s "[\\t]" -1))) 
             (r/map (comp #(Long/parseLong %) first))  
             (r/fold +))))

(time (let [foo (FileSeq. "/tmp/album2.tsv")] 
        (->> foo 
             rest 
             (r/map (fn [^String s] (.split s "[\\t]" -1))) 
             (r/map (comp #(Long/parseLong %) first))  
             (r/fold +))))

(time (let [foo (vec "/tmp/album2.tsv")]  ;; for comparison
        (->> foo 
             rest 
             (r/map (fn [^String s] (.split s "[\\t]" -1))) 
             (r/map (comp #(Long/parseLong %) first))  
             (r/fold +))))


(time (let [foo (FileSeq. "/tmp/test2.tsv")] 
        (->> foo 
             rest 
             (r/map (fn [^String s] (.split s "[\\t]" -1))) 
             (r/map (comp #(Long/parseLong %) first))  
             (r/fold +))))


(time (let [foo (vec "/tmp/test2.tsv")]  ;; for comparison
        (->> foo 
             rest 
             (r/map (fn [^String s] (.split s "[\\t]" -1))) 
             (r/map (comp #(Long/parseLong %) first))  
             (r/fold +))))


(time (let [foo (FileSeq. "/tmp/album.tsv")] 
        (->> foo 
             rest 
             (r/map (fn [^String s] (.split s "[\\t]" -1))) 
             (r/map (comp #(Long/parseLong %) first))  
             (r/fold +))))

(time (def mmap (Mmap. album-filename)))


(.size mmap)

(def ba (byte-array 2000))

(.get mmap ba 0 (dec (count ba)))


(->> ba
 (partition-by #(= 10 %))
 first 
 claas
)

(String. ba 0 (count ba) "UTF-8")

(->> (count ba)
     range
     (take-while)
)

(range (count ba))


;; -- All return ISeq
;; first 
;; if start
;; return until terminator
;; else, return from terminator to next terminator
;; next 
;; more
;; cons




;; end of comments
)
