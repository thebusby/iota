
Available via [clojars](http://clojars.org/search?q=iota)
Current stable version: [iota "1.1.0"]
[![Build Status](https://api.travis-ci.org/thebusby/iota.png?branch=master)](https://travis-ci.org/thebusby/iota)


# Iota

Iota is a Clojure library for reading large text files in memory, and offers the following benefits;
* Tuned for Clojure's reducers, letting you reduce over large files in memory.
* Uses Java NIO's mmap() for rapid IO, and handling files larger than available memory.
* Efficiently stores data as it is represented in the file, and only converts to String when necessary.
* Offers efficient indexing and caching that emulates Clojure's native vector and seq data structures.
* Adjustable buffer sizes for IO and caching, enabling tuning for specific data sets.

Why write this library?
I wanted to be able to use Clojure reducers against large text files to speed up data processing, and without needing more than 10% memory overhead. Due to Java's inefficient storage of [Strings](http://www.javamex.com/tutorials/memory/string_memory_usage.shtml), I found that a 1GB TSV file consumed 10GB of RAM when loaded line by line into a Clojure vector. 


## Details

Iota offers iota/seq and iota/vec for two different use cases. 
Both treat a line, as delimited by a byte separator (default is newline), as an element.

Differences | iota/seq | iota/vec
--- | --- | ---
On creation | Quick, mmap's the file, and stops | Slow, mmap's the file and iterates throught the entire file to generate an index
Sequential access | Scans the buffer for the next byte separator | Quick, N records are read at once and cached 
Random access | O(N), just don't | Quick, O(1) via index
Via reducers | Buffer is divided in half repeatedly until it is smaller than specified size, and then entire buffer is converted to String[] for processing | treated exactly like a Clojure vector, but each thread gets it's own cache though.
Empty line handling | returns "" | returns nil 

Advice; 
* If you'll only be reading the entire file, then use iota/seq. 
* If you need random access across the file, then use iota/vec. 
* If you need random access and line numbers, then iota/numbered-vec.

Note;
* for iota/seq, empty lines will return "".
* for iota/vec, empty lines will return nil.
* for iota/numbered-vec, empty lines will return the line number as a String.


## Usage

```clojure
(def file-vec (iota/vec filename)) ;; Map the file into memory, and generate index of lines. Slow.
(def file-seq (iota/seq filename)) ;; Map the file into memory. Quick.

;; Returns first line of file
(first file-vec) 
(first file-seq)

(last file-vec) ;; Returns last line of file

(nth file-vec 2) ;; Returns the 3rd line of the file

;; Count number of non-empty fields in TSV file
(->> (iota/seq filename)
     (clojure.core.reducers/filter (fn [^String s] (not (.isEmpty s)))) ;; filter out empty lines
     (clojure.core.reducers/map  #(->> (clojure.string/split % #"[\t]" -1)
                                       (filter (fn [^String s] (not (.isEmpty s)))) ;; Remove empty fields
                                       count))
     (clojure.core.reducers/fold  +))

;; Skips the first line of the file, good for ignoring a header
(iota/subvec file-vec 1) 
(rest file-seq) 
```


## Known issues;
* Records must be delimited by a single byte value, hence 2 Byte encodings like UTF-16 and UCS-2 can't be parsed correctly.
* Empty line handling is a mess, this will likely be cleaned up later.


## Artifacts

Iota artifacts are [released to Clojars](https://clojars.org/iota).

If you are using Maven, add the following repository definition to your `pom.xml`:

``` xml
<repository>
  <id>clojars</id>
  <url>http://clojars.org/repo</url>
</repository>
```

### The Most Recent Release

With Leiningen:
```clojure
    [iota "1.1.0"]
```

With Maven:
``` xml
    <dependency>
      <groupId>iota</groupId>
      <artifactId>iota</artifactId>
      <version>1.1.0</version>
    </dependency>
```


## License

CC0
http://creativecommons.org/publicdomain/zero/1.0/

I'd also like to thank my employer Gracenote, for allowing me to create this open source port.

NOTE: Relevant bits of iota/core.clj such as fjinvoke, fjfork, fjjoin, and various bits of iota/FileVector's implementation of Clojure interfaces were copied/modified from Rich Hickey's Clojure. These bits fall under the original owner's license (EPL).


Copyright (C) 2012-2013 Alan Busby
