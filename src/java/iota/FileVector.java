package iota;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;

/* Grab Clojure */
import clojure.lang.APersistentVector;
import clojure.lang.IPersistentVector;
import clojure.lang.IPersistentStack;
import clojure.lang.IPersistentCollection;
import clojure.lang.IPersistentMap;
import clojure.lang.PersistentList;
import clojure.lang.IChunkedSeq;
import clojure.lang.ASeq;
import clojure.lang.ISeq;
import clojure.lang.Obj;
import clojure.lang.Associative;

public class FileVector extends APersistentVector {

    // Static Variables
    final static int  DEFAULT_CHUNK_SIZE = 10;
    final static byte DEFAULT_SEP        = 10; // Newline in ASCII
    final static int  BUFSIZE            = 4096;
    public final static FileVector EMPTY = new FileVector(null, null, FileVector.DEFAULT_CHUNK_SIZE, 0);

    // Member Variables
    public final Mmap             map;
    public final long[]           chunkIndex;
    public final int              chunkSize;
    public final int              lineCount;

    public int                    cachedChunkId;
    public String[]               cachedChunk;




    // Constructors
    //
    public FileVector(String filename) throws IOException {
	this(filename, FileVector.DEFAULT_CHUNK_SIZE);
    }

    public FileVector(String filename, int chunkSize) throws IOException {
	this(filename, FileVector.DEFAULT_CHUNK_SIZE, FileVector.DEFAULT_SEP);
    }

    public FileVector(String filename, int chunkSize, byte sep) throws IOException {
	this.map           = new Mmap( filename );
	this.chunkSize     = chunkSize;
	this.cachedChunkId = -1;

	ArrayList<Long> al = new ArrayList<Long>();
	byte[]         buf = new byte[BUFSIZE];
	long            lc = 0; // Line count
	long           pos = 0; // Position in file
	long      fileSize = map.size(); // Total bytes in file
	int        remsize = 0; // Bytes remaining in buf

	// Capture the start of the file
	al.add( new Long(0) );

	// Index file
	while( pos < fileSize ) {

	    // Populate buffer
	    remsize = (int)Math.min( (long)(fileSize - pos), (long)BUFSIZE );
	    map.get(buf, pos, remsize);

	    // Iterate over every byte in the buffer
	    for(int i=0; i < remsize; i++) {	 // Bytes
		if(buf[i] == sep) {               // Newlines
		    lc++;
		    if ((lc % chunkSize) == 0) { // Chunks
			al.add( new Long(pos + i + 1) );
		    }
		}
	    }
	    pos+=remsize;
	}
	al.add( new Long(pos) ); // Capture the EOF

	// Handle trailing text between \n and EOF
	if(buf[remsize-1] != sep) {
	    lc++;
	}

	// Record line count
	this.lineCount = (int)lc;

	// Convert to long
	long[] tmp = new long[al.size()];
	for(int i=0; i < al.size(); i++)
	    tmp[i] = ((Long)al.get(i)).longValue();

	this.chunkIndex = tmp;
    }

    public FileVector(Mmap map, long[] chunkIndex, int chunkSize, int lineCount) {
	this.map           = map;
	this.chunkSize     = chunkSize;
	this.cachedChunkId = -1;
	this.lineCount     = lineCount;
	this.chunkIndex    = chunkIndex;
    }


    public String[] getChunk(int i) {
	long      pos = chunkIndex[i++];
	int      size = (int)(chunkIndex[i] - pos);
	byte[]    buf = new byte[size];
	String[]   rv = null;

	try {
	    // Grab chunk from memory
	    map.get(buf, pos, size);

	    // Convert to string and split on lines
	    String sbuf = new String(buf, 0, size, "UTF-8");
	    rv = sbuf.split("[\n]", -1);
	}
	catch (UnsupportedEncodingException e)  {
	    // DEBUG / ERROR / ETC
	    // Not handling encoding exceptions properly!!!
	}

	return rv;
    }

    // Retrieve line
    protected String getLine(int i) {
	return getLine(this, i);
    }

    protected String getLine(FileVector v, int i) {
	int  chunk     = i / chunkSize;
	int  chunk_n   = i % chunkSize;
	String   rv    = null;

	if( (chunk < 0) || (chunk >= this.chunkIndex.length) )
	    throw new IndexOutOfBoundsException("getLine() failure: " + chunk + " is not within  0..." + this.chunkIndex.length);

	synchronized(v) {
	    if(chunk != v.cachedChunkId) {
		v.cachedChunk   = v.getChunk(chunk);
		v.cachedChunkId = chunk;
	    }

	    if(chunk_n >= v.cachedChunk.length)
		throw new IndexOutOfBoundsException("getLine() failure: Chunk #" + chunk + "'s [" + chunk_n + "]  is not within  0..." + v.cachedChunk.length);

	    rv = v.cachedChunk[chunk_n];		
	}

	if(rv.isEmpty())
	    return null;
	return rv;
    }


    /*  **  **  **  **  **  **  **  **
     *  FileVector Specific Public Calls
     *  **  **  **  **  **  **  **  **/
    public FileVector subvec(int start){
	return subvec(start, count());
    }

    public FileVector subvec(int start, int end){
	if(end < start || start < 0 || end > this.count())
	    throw new IndexOutOfBoundsException("[" + start + ", " + end + "] not between 0 and " + this.count() + ".");
	if(start == end)
	    return FileVector.EMPTY;
	return new FileVector.SubFileVector(this, start, end);
    }


    /*  **  **  **  **  **  **  **  **
     *  For Clojure compatibility
     *  **  **  **  **  **  **  **  **/
    public class SubFileVector extends FileVector{
	final FileVector v;
	final int        start;
	final int        end;

	public SubFileVector(FileVector v, int start, int end){
	    super(v.map, v.chunkIndex, v.chunkSize, v.lineCount);

	    if(v instanceof FileVector.SubFileVector)
		{
		    FileVector.SubFileVector sv = (FileVector.SubFileVector)v;
		    start += sv.start;
		    end += sv.start;
		    v = sv.v;
		}
	    this.v = v;
	    this.start = start;
	    this.end = end;
	}

	public String nth(int i){
	    if((start + i >= end) || (i < 0))
		throw new IndexOutOfBoundsException("bad value for " + i + " since it's really " + start + " + " + i + " which is outside " + end);

	    // Want to call provided FileVector's function, which may be overloaded, but 
	    // against this class's data so it uses an independent cache
	    return v.getLine(this, start + i);
	}

	public int count(){
	    return end - start;
	}

	public ISeq seq(){
	    return new FileVectorSeq(this);
	}
    }


    /* For clojure.lang.Indexed */
    public String nth(int i){
	return getLine(i);
    }

    /* For clojure.lang.Indexed */
    public String nth(int i, Object notFound){
	String s = nth(i);
	if(s == null)
	    return (String)notFound;
	return s;
    }

    /* For clojure.lang.Counted */
    public int count(){
	return this.lineCount;
    }

    public String first(){
	return nth(0);
    }

    /* To protent against changes */
    public IPersistentVector assocN(int i, Object val){
	throw new java.lang.UnsupportedOperationException();
    }

    /* For clojure.lang.IPersistentVector */
    public IPersistentVector cons(Object o){
	throw new java.lang.UnsupportedOperationException();
    }

    public Object[] arrayFor(int i){
	throw new java.lang.UnsupportedOperationException();
    }

    public IChunkedSeq chunkedSeq(){
	throw new java.lang.UnsupportedOperationException();
    }

    public ISeq seq(){
    	return new FileVectorSeq(this);
    }

    public Iterator iterator(){
	return rangedIterator(0, count());
    }

    /* For clojure.lang.SubFileVector's iterator() */
    Iterator rangedIterator(final int start, final int end){
	return new Iterator(){
	    int    i = start;

	    public boolean hasNext(){
		return i < end;
	    }

	    public Object next(){
		return nth(i++);
	    }

	    public void remove(){
		throw new UnsupportedOperationException();
	    }
	};
    }

    public FileVector pop(){
	throw new UnsupportedOperationException();
    }

    public IPersistentCollection empty(){
	return EMPTY;
    }

    static public final class FileVectorSeq extends ASeq{
	FileVector v;
	int    i;

	public FileVectorSeq(FileVector v){
	    this.v = v;
	    this.i = 0;
	}

	public FileVectorSeq(FileVector v, int i){
	    this.v = v;
	    this.i = i;
	}

	public Object first(){
	    if (this.i < v.count())
		return v.nth(i);
	    return null;
	}

	public ISeq next(){
	    if ((this.i + 1) < v.count())
		return new FileVectorSeq(this.v, (this.i + 1) );
	    return null;
	}

	public int count(){
	    return v.count() - i;
	}

	public ISeq cons(Object o){
	    throw new UnsupportedOperationException();
	}

	public Obj withMeta(IPersistentMap meta){
	    throw new UnsupportedOperationException();
	}
    }
}


