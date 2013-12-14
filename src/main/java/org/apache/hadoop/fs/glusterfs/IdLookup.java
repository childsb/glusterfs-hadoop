package org.apache.hadoop.fs.glusterfs;

import java.io.IOException;

import java.io.InputStream;

public class IdLookup {
	
	public static final int UID = 1;
	public static final int GID = 2;
	
	public static int getUid(String name) throws IOException{
		return getId(name, UID);
	}
	
	public static int getGid(String name) throws IOException{
		return getId(name, GID);
	}
	
	protected static int getId(String name, int type){

	    String userName = System.getProperty("user.name");
	    String arg = null;
		    
	    switch(type){
	    	case IdLookup.UID : 
	    		arg = "-u";
    		break;
		    		
		    case IdLookup.GID :
		    	arg = "-g";
		    break;
		}
		    
		String command = "id "+ arg + " " + userName;
		Process child = null;
		try {
			child = Runtime.getRuntime().exec(command);
		} catch (IOException e) {
			
			e.printStackTrace();
		}
		
		// Get the input stream and read from it
		InputStream in = child.getInputStream();
		String output = new String();
		int c;
		try{
			while ((c = in.read()) != -1) {
			    output+=c;
			}
			in.close();
		}catch(IOException ex){
			
		}
		return Integer.parseInt(output);
		
	}
	
	public static String getName(int id){
		 String command = "getent passwd | awk -F: '$3 == " + id + " { print $1 }'";
		 Process child=null;
		try {
			child = Runtime.getRuntime().exec(command);
		} catch (IOException e) {
			e.printStackTrace();
		}
		InputStream in = child.getInputStream();
		String output = new String();
		int c;
		try{
		while ((c = in.read()) != -1) {
		    output+=c;
		}
		in.close();
		}catch(IOException ex){
			
		}
		return output;
		
	}

}
