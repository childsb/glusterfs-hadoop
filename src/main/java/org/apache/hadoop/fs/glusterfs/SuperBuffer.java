package org.apache.hadoop.fs.glusterfs;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class SuperBuffer extends BufferedOutputStream {

	public SuperBuffer(OutputStream out) {
		super(out);
		
	}
	 public SuperBuffer(OutputStream out, int size) {
	       super(out,size);
	 }
    public synchronized void write(byte b[], int off, int len) throws IOException {
        for(int i=off;i<len;i++){
        	super.write(b[i]);
        }
    }
}
