package iota;

import java.io.*;

/* Grab Clojure */
import clojure.lang.ASeq;
import clojure.lang.ISeq;
import clojure.lang.IPersistentMap;
import clojure.lang.Obj;


public class FileSeq extends ASeq {

    // Static Variables
    final static int  DEFAULT_BUFSIZE = 262144; // 256KB
    final static byte DEFAULT_LINESEP = 10; // ASCII newline

    // Member Variables
    public final Mmap map;
    public final long start;
    public final long end;
    public final int  bufsize;
    public final byte linesep;

    public FileSeq(String filename) throws IOException {
	this(filename, FileSeq.DEFAULT_BUFSIZE, FileSeq.DEFAULT_LINESEP);
    }

    public FileSeq(String filename, int bufsize) throws IOException {
	this(filename, bufsize, FileSeq.DEFAULT_LINESEP);
    }

    public FileSeq(String filename, int bufsize, byte sep) throws IOException {
	long   end = 0;
	byte[] buf = new byte[1];
	Mmap   map = new Mmap(filename);
	
	this.map     = map;
	this.start   = 0;
	this.bufsize = bufsize;
	this.linesep = sep;

	// Handle trailing separator
	end = map.size();
	map.get(buf, (end-1), 1);
	if(buf[0] == sep)
	    end--;

	this.end = end;
    }

    public FileSeq(Mmap map, long start, long end, int bufsize, byte sep) {
	this.map     = map;
	this.start   = start;
	this.end     = end;
	this.bufsize = bufsize;
	this.linesep = sep;
    }

    // Fairly inefficient, hope this isn't used too much...
    public long mapchr(long start, long end, byte b, long rv) {
	byte[] buf = new byte[bufsize];

	for(long i = start; i < end; i+=bufsize)
	    {
		int remsize = (int)Math.min( (long)(this.end - i), (long)bufsize );
		map.get(buf, i, remsize);

		for(int x=0; x < remsize; x++) {
		    if (buf[x] == linesep) {
			return (i+x);
		    }
		}
	    }

	return rv;
    }

    public FileSeq[] split(){
	FileSeq[] rv = new FileSeq[2];

	// Only split if buffer is larger than BUFSIZE
	if((end - start) < bufsize) {
	    return null;
	}

	// Find midpoint
	long midpoint = mapchr((((end - start) / 2) + start), end, linesep, -2) + 1;
	if((midpoint < 0) || (midpoint >= end)) {
	    return null;
	}

	// Create new for left and right
	rv[0] = new FileSeq(map, start, (midpoint-1), bufsize, linesep); // minus one to remove newline
	rv[1] = new FileSeq(map, midpoint, end,       bufsize, linesep);
	return rv;
    }

    public Object first() {
	long eol = mapchr(start, end, linesep, end);
	int size = (int)(eol - start);
	byte[] buf = new byte[size];
	map.get(buf, start, size);
	String rv = null;

	try {
	    rv = new String(buf, 0, size, "UTF-8");
	} catch (UnsupportedEncodingException e)  {
	    // DEBUG / ERROR / ETC
	    // Not handling encoding exceptions properly!!!
	}

	return rv;
    }

    public ISeq next() {
	long eol = mapchr(start, end, linesep, -2) + 1;

	if( (eol < 0) || (eol >= end)) {
	    return null;
	}

	return new FileSeq(this.map, eol, end, bufsize, linesep);
    }

    public Object[] toArray() {
	int size = (int)(end - start); // this better be smaller than 2GB!
	byte[] buf = new byte[size];
	String[] rv = null;

	map.get(buf, start, size);

	try {
	    String sbuf = new String(buf, 0, size, "UTF-8");
	    rv = sbuf.split("[\n]", -1);
	} catch (UnsupportedEncodingException e)  {
	    // DEBUG / ERROR / ETC
	    // Not handling encoding exceptions properly!!!
	}

	return rv;
    }

    public Obj withMeta(IPersistentMap meta){
    	throw new UnsupportedOperationException();
    }
}
