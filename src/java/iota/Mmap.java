package iota;

import java.io.*;

import java.util.ArrayList;
import java.nio.channels.FileChannel;
import java.nio.MappedByteBuffer;


public class Mmap {

    public static class FileWrapper{
	public final FileInputStream    fis;
	public final FileChannel        fc;

	public FileWrapper(String filename) throws IOException {
	    this.fis = new FileInputStream( filename );
	    this.fc = fis.getChannel();
	}

	protected void finalize() throws Throwable {
	    try {
		fc.close();
		fis.close();
	    } finally {
		super.finalize();
	    }
	}
    }

    // private static final long        MAP_SIZE = Integer.MAX_VALUE;
    private static final long        MAP_SIZE = 2000000000; // 2B
    private final FileWrapper        fw;
    private final MappedByteBuffer[] mbs;

    public Mmap(String filename) throws IOException{
	long       pos = 0;

	this.fw = new FileWrapper( filename );

	// Map file into multiple buffers
	ArrayList<MappedByteBuffer> al = new ArrayList<MappedByteBuffer>();

	while(pos < fw.fc.size()) {
	    long toMap = Math.min( (fw.fc.size() - pos) , MAP_SIZE );

	    al.add( fw.fc.map( FileChannel.MapMode.READ_ONLY, pos, toMap ) );

	    pos += toMap; 
	}

	MappedByteBuffer[] tmp = new MappedByteBuffer[al.size()];
	mbs = (MappedByteBuffer[])al.toArray(tmp);
    }

    public Mmap(FileWrapper fw, MappedByteBuffer[] mbs) {
	this.fw   = fw;
	this.mbs  = mbs;
    }

    public Mmap duplicate() {
	ArrayList<MappedByteBuffer> al = new ArrayList<MappedByteBuffer>();
	for(int i = 0; i < mbs.length; i++)
	    al.add( (MappedByteBuffer)mbs[i].duplicate() );

	MappedByteBuffer[] tmp = new MappedByteBuffer[al.size()];
	return new Mmap(fw, (MappedByteBuffer[])al.toArray(tmp));
    }

    public void get(byte[] buffer, long pos, int size) {
	int chunk           = (int)(pos / MAP_SIZE);
	int chunk_n         = (int)(pos % MAP_SIZE);
	MappedByteBuffer mb = mbs[chunk];
	long readEnd        = (((long)chunk_n) + ((long)size));
	int  readSize       = (int)((readEnd <= MAP_SIZE) ? size : (MAP_SIZE - chunk_n));

	synchronized(mb) {
	    mb.position(chunk_n);
	    mb.get(buffer, 0, readSize);
	}

	if(readEnd > MAP_SIZE) {
	    mb = mbs[++chunk];
	    synchronized(mb) {
		mb.position(0);
		mb.get(buffer, readSize, (int)(readEnd - MAP_SIZE));
	    }
	}
    }

    public long size() throws IOException {
	return this.fw.fc.size();
    }

    public static void main (String[] args) throws IOException
    {
	Mmap mmap = new Mmap(args[0]);

	System.out.println(args[0] + " mmap()'d over " + mmap.size() + "B");
    }
}
