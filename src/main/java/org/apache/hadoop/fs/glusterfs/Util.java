/**
 *
 * Copyright (c) 2013 Red Hat, Inc. <http://www.redhat.com>
 * This file is part of GlusterFS.
 *
 * Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * Implements the Hadoop FileSystem Interface to allow applications to store
 * files on GlusterFS and run Map/Reduce jobs on the data.
 * 
 * 
 */

package org.apache.hadoop.fs.glusterfs;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.util.Shell;

public class Util{
	
	public static File flushAcl(File p) throws IOException{
		 String aclBackupCommand="getfacl -pPdEsR ";
	     File tempFile = File.createTempFile("glusterfsACL-" + System.currentTimeMillis() , ".tmp", new File(System.getProperty("java.io.tmpdir")));
	     String command = aclBackupCommand + p.getAbsolutePath() + " >"+ tempFile.getAbsoluteFile();
	     Process proc = Runtime.getRuntime().exec(command);
	     BufferedReader brInput=new BufferedReader(new InputStreamReader(proc.getInputStream()));
	     FileWriter fw = new FileWriter(tempFile);
	     BufferedWriter bw = new BufferedWriter(fw);
		 String s;
	            
	     while ((s=brInput.readLine())!=null){
	    	 bw.write(s + System.getProperty("line.separator"));
	     }
	     
	     bw.close();
	     
	     return tempFile;
	                  
	}
	
	public static void restoreAcl(File p) throws IOException{
		String aclRestoreCommand="setfacl --restore=";
		String command = aclRestoreCommand + p.getAbsolutePath();
       Runtime.getRuntime().exec(command);
	}
	
    public static String execCommand(File f,String...cmd) throws IOException{
        String[] args=new String[cmd.length+1];
        System.arraycopy(cmd, 0, args, 0, cmd.length);
        args[cmd.length]=FileUtil.makeShellPath(f, true);
        String output=Shell.execCommand(args);
        return output;
    }

    /* copied from unstalbe hadoop API org.apache.hadoop.Shell */
    public static String[] getGET_PERMISSION_COMMAND(){
        // force /bin/ls, except on windows.
        return new String[]{(WINDOWS ? "ls" : "/bin/ls"),"-ld"};
    }
    
    /* copied from unstalbe hadoop API org.apache.hadoop.Shell */
    
    public static final boolean WINDOWS /* borrowed from Path.WINDOWS */
    =System.getProperty("os.name").startsWith("Windows");
    // / loads permissions, owner, and group from `ls -ld`
}
