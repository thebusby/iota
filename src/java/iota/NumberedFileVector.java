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
	String line = super.getLine(v,i);	

	if(line!=null){
	    return i + this.delim + line;
	}
	return i + "";
    }
}
