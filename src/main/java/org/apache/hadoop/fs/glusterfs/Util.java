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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.util.Shell;

public class Util{
	 private static File logFile=null;
	    private static final String LOG_PREFIX="/tmp/glusterfs";
	    private static boolean showStackTrace = true;
	    
	    public static synchronized String getStackTrace(int stripTopElements){
	        StackTraceElement[] trace = Thread.currentThread().getStackTrace();
	        String traceString = "";
	        // remove the specified top elements of the stack trace (to avoid debug methods in the trace). the +1 is for this methods call.
	        for(int i=stripTopElements+1;i<trace.length;i++){
	            traceString += "\t[" + trace[i].getFileName() + "] " +  trace[i].getClassName() +  "." + trace[i].getMethodName() +  "() line:" + trace[i].getLineNumber() + "\n";
	        }
	        
	        return traceString;
	    }
	    
	    public static synchronized void logMachine(String text){

	        DateFormat dateFormat=new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
	        Date date=new Date();
	        if(logFile==null){
	            for(int i=0;i<1000000;i++){
	                logFile=new File(LOG_PREFIX+"-"+i+".log");
	                if(!logFile.exists()){
	                    try{
	                        logFile.createNewFile();
	                        break;
	                    }catch (IOException e){
	                    }
	                }
	            }
	        }

	        try{
	            PrintWriter out=new PrintWriter(new BufferedWriter(new FileWriter(logFile, true)));
	            out.write("(" + dateFormat.format(date)+") : "+text+"\n");
	            String stackTrace = Util.getStackTrace(3);
	            if(showStackTrace) out.write(stackTrace);
	            out.close();
	        }catch (FileNotFoundException e){
	            e.printStackTrace();
	        }catch (IOException e){
	            e.printStackTrace();
	        }

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
