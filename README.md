# Iota

Iota is a simple library which indexes text files via Java NIO's mmap() to produce a vector like data structure. This allows for using Clojure 1.5's reducers to operate over text files larger than memory in parallel. Each element in the iota vector is associated with a single line in the file, and O(1) lookup speed are maintained via a simple index. 

This not only enables the use of reducers, for greatly improved parallel performance; but allows large text files to be stored in memory without Java doubling or even tripling the amount of memory required (due to unicode encodings, and java.lang.String overhead).

Note: 
for iota/vec, empty lines will return nil.
for iota/numbered-vec, empty lines will return the line number as a String.


## Usage
```clojure
(def mem-file (iota/vec filename)) ;; Map the file into memory, and generate index of lines

(first mem-file) ;; Returns first line of file

(last mem-file) ;; Returns last line of file

(nth mem-file 2) ;; Returns the 3rd line of the file

;; Count number of non-empty fields in TSV file
(->> (iota/vec filename)	
     (clojure.core.reducers/filter identity) ;; filter out empty lines
     (clojure.core.reducers/map  #(->> (clojure.string/split % #"[\t]" -1)
                                       (filter (fn [^String s] (not (.isEmpty s))))
                                       count)) 
     (clojure.core.reducers/fold  +))

(iota/subvec mem-file 1) ;; Skips the first line of the file, good for ignoring a header

```



## License

CC0
http://creativecommons.org/publicdomain/zero/1.0/

I'd also like to thank my employer Gracenote, for allowing me to create this open source port.

NOTE: Relevant bits of iota/core.clj such as fjinvoke, fjfork, fjjoin, and various bits of iota/FileVector's implementation of Clojure interfaces were copied/modified from Rich Hickey's Clojure. These bits fall under the original owner's license (EPL at the time of writing).


Copyright (C) 2012 Alan Busby


