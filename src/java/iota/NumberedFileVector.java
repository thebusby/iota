package iota;

import java.io.IOException;

public class NumberedFileVector extends FileVector {

    private final String delim;

    // Constructors
    //
    public NumberedFileVector(String filename, int chunkSize, String delim) throws IOException  {
	super(filename, chunkSize);
	this.delim = delim;
    }

    // Append line number to beginning of line
    protected String getLine(FileVector v, int i) {
	return i + this.delim + super.getLine(v,i);
    }
}
