/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapred;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.util.Shell;

/**
 * A utility class. It provides
 *   - file-util
 *     - A path filter utility to filter out output/part files in the output dir
 */
public class Utils {
  // added by Lijie Xu
    private static final Log LOG =
	    LogFactory.getLog(Utils.class);
  // added end
    
  public static class OutputFileUtils {
    /**
     * This class filters output(part) files from the given directory
     * It does not accept files with filenames _logs and _SUCCESS.
     * This can be used to list paths of output directory as follows:
     *   Path[] fileList = FileUtil.stat2Paths(fs.listStatus(outDir,
     *                                         new OutputFilesFilter()));
     */
    public static class OutputFilesFilter extends OutputLogFilter {
      public boolean accept(Path path) {
        return super.accept(path) 
               && !FileOutputCommitter.SUCCEEDED_FILE_NAME
                   .equals(path.getName());
      }
    }
    
    /**
     * This class filters log files from directory given
     * It doesnt accept paths having _logs.
     * This can be used to list paths of output directory as follows:
     *   Path[] fileList = FileUtil.stat2Paths(fs.listStatus(outDir,
     *                                   new OutputLogFilter()));
     */
    public static class OutputLogFilter implements PathFilter {
      public boolean accept(Path path) {
        return !(path.toString().contains("_logs"));
      }
    }
  }
  
  // added by Lijie Xu
  public static void heapdump(String dumppath, String name) {
	String pid = "";

	if (!Shell.WINDOWS) {
	   pid = System.getenv().get("JVM_PID");
	}
	
	if(pid.isEmpty())
	    return;
	
	String dumpComm = "exec $JAVA_HOME/bin/jmap -dump:live,file=" + dumppath 
		+ File.separatorChar + name + "-pid-" + pid + ".hprof " + pid;

		
	Process p = null;
	
	try {
	    p = new ProcessBuilder("bash", "-c", dumpComm).start();
	    int exitCode = p.waitFor();
	    LOG.info("[HeapDump] generate " + dumppath 
		+ File.separatorChar + name + "-pid-" + pid + ".hprof");
	} catch (IOException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	} catch (InterruptedException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	} finally {
	    p.destroy();
	}		     		
  }
  
  public static long[] parseHeapDumpConfs(String heapDumpConf) {
      if(heapDumpConf == null) 
	  return null;
      else {
	  String[] limits = heapDumpConf.split(",");
	  long[] values = new long[limits.length];
	  for(int i = 0; i < limits.length; i++) {
	      values[i] = Long.parseLong(limits[i].trim());
	  }
	  return values;
      }
  }
  
  public static Set<String> parseTaskIds(String tasksIdsConf) {
      if(tasksIdsConf == null)
	  return null;
      Set<String> s = new HashSet<String>();
      String[] ids = tasksIdsConf.split(",");
      
      
      for(String id: ids) {
	  int loc = id.indexOf("_m_");
	  if(loc == -1) 
	      loc = id.indexOf("_r_");
	  loc++;
	  String simpleId = id.substring(loc, id.lastIndexOf('_'));
	  s.add(simpleId);
      }
	  
      return s;
  }
  
  public static boolean isSetContainsId(Set<String> profileTaskIds, String taskAttemptId) {
      int loc = taskAttemptId.indexOf("_m_");
      if(loc == -1)
	  loc = taskAttemptId.indexOf("_r_");
      loc++;
      taskAttemptId = taskAttemptId.substring(loc, taskAttemptId.lastIndexOf('_'));
      
      if(profileTaskIds.contains(taskAttemptId))
	  return true;
      else
	  return false;
  }
  
  // added end
}

