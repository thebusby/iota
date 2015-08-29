package iota;

import clojure.lang.ASeq;
import clojure.lang.IPersistentMap;
import clojure.lang.ISeq;
import clojure.lang.Obj;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;

/*
* Variation of FileSeq with multibyte split separator.
* This version will not strip anything out from data
* */


public class FileRecordSeq extends ASeq {

    // Static Variables
    final static int DEFAULT_BUFSIZE = 262144; // 256KB
    final static byte[] DEFAULT_SPLIT_SEP = {10}; // ASCII newline

    // Member Variables
    public final Mmap map;
    public final long start;
    public final long end;
    public final int bufsize;
    public final byte[] splitsep;

    public FileRecordSeq(String filename) throws IOException {
        this(filename, FileRecordSeq.DEFAULT_BUFSIZE, FileRecordSeq.DEFAULT_SPLIT_SEP);
    }

    public FileRecordSeq(String filename, int bufsize) throws IOException {
        this(filename, bufsize, FileRecordSeq.DEFAULT_SPLIT_SEP);
    }

    public FileRecordSeq(String filename, int bufsize, byte[] splitsep) throws IOException {
        Mmap map = new Mmap(filename);

        this.map = map;
        this.start = 0;
        this.bufsize = bufsize;
        this.splitsep = splitsep;
        this.end = map.size();
    }

    public FileRecordSeq(Mmap map, long start, long end, int bufsize, byte[] splitsep) {
        this.map = map;
        this.start = start;
        this.end = end;
        this.bufsize = bufsize;
        this.splitsep = splitsep;
    }

    private static class Chunk {
        public int start;
        public int end;
        public byte[] buf;
        public boolean foundSplit;
        public int matchIndex;

        public Chunk(byte[] buf) {
            this.buf = buf;
            setValues(0, 0, false, 0);
        }

        public Chunk resetPosition() {
            start = 0;
            end = 0;
            return this;
        }

        public Chunk setValues(int start, int end, boolean foundSplit, int matchIndex) {
            this.start = start;
            this.end = end;
            this.foundSplit = foundSplit;
            this.matchIndex = matchIndex;
            return this;
        }
    }

    private Chunk nextSplit(Chunk chunk, int end, byte[] sep) {
        // need to match separators also on the border of chunks
        int matchIndex = chunk.matchIndex;
        boolean lastMatch;
        byte[] buf = chunk.buf;

        for (int x = chunk.end; x < end; x++) {

            if (buf[x] == sep[matchIndex]) {
                lastMatch = true;
            } else {
                lastMatch = false;
                matchIndex = 0;
            }

            if (lastMatch && matchIndex == sep.length - 1) {
                return chunk.setValues(chunk.end, x + 1, true, 0);
            } else if (lastMatch) {
                matchIndex++;
            }
        }
        // separator not found, return the whole remaining chunk
        return chunk.setValues(chunk.end, end, false, matchIndex);
    }

    private long nextChunkEnd(long start, long end, byte[] sep) {

        byte[] buf = new byte[bufsize];
        Chunk chunk = new Chunk(buf);
        for (long i = start; i < end; i += bufsize) {
            int remsize = Math.min((int) (end - i), bufsize);

            map.get(buf, i, remsize);

            chunk = nextSplit(chunk.resetPosition(), remsize, sep);
            if (chunk.foundSplit) {
                return i + chunk.end;
            }
        }

        return -1;
    }

    public FileRecordSeq[] split() {
        // Find midpoint
        return split((((end - start) / 2) + start));
    }

    public FileRecordSeq[] split(long loc) {

        // Only split if buffer is larger than BUFSIZE
        if ((end - start) < bufsize) {
            return null;
        }

        long eor = nextChunkEnd(loc, end, splitsep);
        if (eor == -1 || eor >= end) {
            return null;
        }

        FileRecordSeq[] rv = new FileRecordSeq[2];
        // Create new for left and right
        rv[0] = new FileRecordSeq(map, start, eor, bufsize, splitsep);
        rv[1] = new FileRecordSeq(map, eor, end, bufsize, splitsep);
        return rv;
    }

    public Object first() {
        long eor = nextChunkEnd(start, end, splitsep);
        eor = eor == -1 ? end : eor;
        int size = (int)(eor - start);
        byte[] buf = new byte[size];
        map.get(buf, start, size);
        String rv = null;

        try {
            rv = new String(buf, 0, size, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            // DEBUG / ERROR / ETC
            // Not handling encoding exceptions properly!!!
        }

        return rv;
    }

    public ISeq next() {
        long eor = nextChunkEnd(start, end, splitsep);

        if (eor == -1 || eor >= end) {
            return null;
        }

        return new FileRecordSeq(this.map, eor, end, bufsize, splitsep);
    }

    public Object[] toArray() {
        int size = (int)(end - start); // this better be smaller than 2GB!
        byte[] buf = new byte[size];
        map.get(buf, start, size);
        ArrayList<String> rv = new ArrayList<String>();

        try {
            Chunk chunk = new Chunk(buf);
            do {
                chunk = nextSplit(chunk, size, splitsep);
                String chunkStr = new String(chunk.buf, chunk.start, chunk.end - chunk.start, "UTF-8");
                rv.add(chunkStr);
            } while (chunk.end < size);

        } catch (UnsupportedEncodingException e) {
            // DEBUG / ERROR / ETC
            // Not handling encoding exceptions properly!!!
        }

        return rv.toArray();
    }

    public Obj withMeta(IPersistentMap meta) {
        throw new UnsupportedOperationException();
    }
}
