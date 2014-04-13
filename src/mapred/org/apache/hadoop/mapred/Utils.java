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
	int exitCode = 0;
	
	try {
	    p = new ProcessBuilder("bash", "-c", dumpComm).start();
	    exitCode = p.waitFor();
	    LOG.info("[HeapDump] generate " + dumppath 
		+ File.separatorChar + name + "-pid-" + pid + ".hprof");
	} catch (IOException e) {
	    LOG.info("[Dump error] exitCode = " + exitCode + " " + dumppath 
		+ File.separatorChar + name + "-pid-" + pid + ".hprof");
	    e.printStackTrace();
	} catch (InterruptedException e) {
	    LOG.info("[Dump error] exitCode = " + exitCode + " " + dumppath 
			+ File.separatorChar + name + "-pid-" + pid + ".hprof");
	    e.printStackTrace();
	} finally {
	    p.destroy();
	}		     		
  }
  
  /* headDumpConfiguration:
   * 1. counter[partition] ==> interval, 2 * interval, ..., end - 1  ----- interval = (counter - 1) / partition
   *		e.g., 133,200,300[10] ==> 13320029, 26640058, 39960087, 53280116, 66600145,
   *				          79920174, 93240203, 106560232, 119880261, 133200299
   * 
   * 2. (begin : end)[partition] ==> begin, begin + interval, begin + 2 * interval, ..., end - 1
   * 		e.g., (1,000,000 : 133,200,300)[10] ==> 1000000, 14220029, 27440058, 40660087, 53880116,
   * 							67100145, 80320174, 93540203, 106760232, 119980261,
   *						        133200298
   *
   * 3. counter{interval} ==> interval, 2 * interval, 3 * interval, ..., n * interval (<= counter - 1)
   * 		e.g., (1,000,000 : 133,200,300)[10] ==> 13320029, 26640058, 39960087, 53280116, 66600145,
   *				                        79920174, 93240203, 106560232, 119880261, 133200299
   */
  
  public static long[] parseHeapDumpConfs(String heapDumpConf) {
	if (heapDumpConf == null)
	    return null;
	// 133,200,300[10] or 133200300[10]
	else if(heapDumpConf.contains("[") && !heapDumpConf.contains(":")) {
	    int loc = heapDumpConf.indexOf('[');
	    long maxValue = Long.parseLong(heapDumpConf.substring(0, loc).replaceAll(",", "").trim()) - 1;
	    int partition = Integer.parseInt(heapDumpConf.substring(loc + 1, 
		   heapDumpConf.lastIndexOf(']')).replaceAll(",", "").trim());
	    long interval = (long) (maxValue / partition);
	    long[] values = new long[partition];
	   
	    for(int i = 0; i < partition - 1; i++) 
	        values[i] = (i + 1) * interval;
	    values[partition - 1] = maxValue;
	   
	    return values;
	   
	}
	
	// (1,000,000 : 133,200,300)[10]
	else if(heapDumpConf.contains(":")) {
	    
	    String minValueStr = heapDumpConf.substring(heapDumpConf.indexOf('(') + 1, heapDumpConf.indexOf(':'))
		    .replaceAll(",", "").trim();
	    String maxValueStr = heapDumpConf.substring(heapDumpConf.indexOf(':') + 1, heapDumpConf.indexOf(')'))
		    .replaceAll(",", "").trim();
	    long minValue = Long.parseLong(minValueStr);
	    long maxValue = Long.parseLong(maxValueStr) - 1;
	    
	    int partition = Integer.parseInt(heapDumpConf.substring(heapDumpConf.indexOf('[') + 1, 
			   heapDumpConf.lastIndexOf(']')).replaceAll(",", "").trim());
	    long interval = (long) ((maxValue - minValue) / partition);
	    long[] values = new long[partition + 1];
		   
	   
	    for(int i = 0; i < partition; i++) 
		values[i] = i * interval + minValue;
	    values[partition] = maxValue - 1;
		   
	    return values;
		   
	}
	// 133,200,300{10,000,000} or 133200300{10000000}
	else if(heapDumpConf.contains("{")) {
	    int loc = heapDumpConf.indexOf('{');
	    long maxValue = Long.parseLong(heapDumpConf.substring(0, loc).replaceAll(",", "").trim()) - 1;
	    long interval = Long.parseLong(heapDumpConf.substring(loc 
		    + 1, heapDumpConf.lastIndexOf('}')).replaceAll(",", "").trim());
	    int partition = (int) (maxValue / interval);
	    if(interval * partition < maxValue)
		partition++;
	    long[] values = new long[partition];
	    
	    for(int i = 0; i < partition - 1; i++)
		values[i] = (i + 1) * interval;
	    values[partition - 1] = maxValue;
	    
	    return values;
	    
	    
	}
	
	// 133,200,300
	else {
	    String[] limits = heapDumpConf.split("(\\.|-|/|;)");
	    long[] values = new long[limits.length];
	    for (int i = 0; i < limits.length; i++) {
		values[i] = Long.parseLong(limits[i].replaceAll(",", "").trim());
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

