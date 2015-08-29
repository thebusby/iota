package iota;

import clojure.lang.ASeq;
import clojure.lang.IPersistentMap;
import clojure.lang.ISeq;
import clojure.lang.Obj;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;

/*
* Variation of FileSeq which will produce sequence of arrays of data.
*
* Internally uses FileRecordSeq.
*
* This is intended for fold usages that operates on iterables rather
* than core.reducers CollFold protocol.
*
* One example is Tesser https://github.com/aphyr/tesser.
* */


public class FileChunkSeq extends ASeq {

    private final FileRecordSeq seq;
    private FileRecordSeq[] parts = null;

    public FileChunkSeq(FileRecordSeq seq) {
        this.seq = seq;
    }

    public Object first() {
        if (parts == null) {
            parts = seq.split(seq.start + seq.bufsize);
        }
        if (parts != null) {
            return parts[0].toArray();
        }
        return seq.toArray();
    }

    public ISeq next() {
        if (parts == null) {
            parts = seq.split(seq.start + seq.bufsize);
        }
        if (parts == null) {
            return null;
        }
        return new FileChunkSeq(parts[1]);
    }

    public Obj withMeta(IPersistentMap meta) {
        throw new UnsupportedOperationException();
    }
}
