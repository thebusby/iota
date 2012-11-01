# Iota

Iota is a simple library that applies a wrapper over Java NIO's mmap() functionalionality to produce a vector like object compatible with Clojure 1.5's reducers.


## Usage

(def mem-file (fvec "filename))

(first mem-file)

(last mem-file)

(->> mem-file
     (clojure.core.reducers/map  #(->> (clojure.string/split % #"[\t]" -1) 
                                       (filter (fn [^String x] (not (.isEmpty x)))) 
                                       count)) 
     (clojure.core.reducers/fold  +))

etc.


## License

CC0
http://creativecommons.org/publicdomain/zero/1.0/

NOTE: Relevant bits of iota/core.clj such as fjinvoke, fjfork, fjjoin, and various bits of iota/FileVector's implementation of Clojure interfaces were copied/modified from Rich Hickey's Clojure. These bits fall under the original owner's license (EPL at this time).


Copyright (C) 2012 Alan Busby


