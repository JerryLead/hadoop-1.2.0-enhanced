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
import java.util.Set;

import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Shell;


/** Default {@link MapRunnable} implementation.*/
public class MapRunner<K1, V1, K2, V2>
    implements MapRunnable<K1, V1, K2, V2> {
  
  private Mapper<K1, V1, K2, V2> mapper;
  private boolean incrProcCount;
  
  // added by Lijie Xu
  private long[] mapinrecordslimits;
  private String dumppath;
  private Set<String> profileTaskIds;
  
  private Set<String> monitorTasksIds;
  private long monitorMapInterval = 0;
  // added end
  
  @SuppressWarnings("unchecked")
  public void configure(JobConf job) {
    this.mapper = ReflectionUtils.newInstance(job.getMapperClass(), job);
    //increment processed counter only if skipping feature is enabled
    this.incrProcCount = SkipBadRecords.getMapperMaxSkipRecords(job)>0 && 
      SkipBadRecords.getAutoIncrMapperProcCount(job);
    
    // added by Lijie Xu
    mapinrecordslimits = Utils.parseHeapDumpConfs(job.get("heapdump.map.input.records"));
    dumppath = job.get("heapdump.path", "/tmp");
    
    profileTaskIds = Utils.parseTaskIds(job.get("heapdump.task.attempt.ids"));
    
    monitorTasksIds = Utils.parseTaskIds(job.get("monitor.task.attempt.ids"));
    monitorMapInterval = job.getLong("monitor.record.map.interval", 0);
    
    // added end
  }

  public void run(RecordReader<K1, V1> input, OutputCollector<K2, V2> output,
                  Reporter reporter, TaskAttemptID taskAttemptID)
    throws IOException {
    try {
      // allocate key & value instances that are re-used for all entries
      K1 key = input.createKey();
      V1 value = input.createValue();
      
      // modified by Lijie Xu
      if(profileTaskIds != null && !Utils.isSetContainsId(profileTaskIds, taskAttemptID.toString()))
	  mapinrecordslimits = null;
      
      
      
      if(monitorTasksIds != null && !Utils.isSetContainsId(monitorTasksIds, taskAttemptID.toString())) {
	  monitorMapInterval = 0;
      }
      
      
      
      if(mapinrecordslimits == null && monitorMapInterval == 0) {
	  while (input.next(key, value)) {
		  
	        // map pair to output
	        mapper.map(key, value, output, reporter);
	        if(incrProcCount) {
	          reporter.incrCounter(SkipBadRecords.COUNTER_GROUP, 
	              SkipBadRecords.COUNTER_MAP_PROCESSED_RECORDS, 1);
	        }
	  } 
      }
      
      else if (mapinrecordslimits == null && monitorMapInterval != 0){
	  
	MemoryMonitor.recordInterval = monitorMapInterval;
	MemoryMonitor.mapMonitorThread.start();
	    
	while (input.next(key, value)) {

	    MemoryMonitor.addRecord();
	    MemoryMonitor.monitorAfterProcessRecord();
	    MemoryMonitor.monitorBeforeMapProcessRecord();
	    
	    // map pair to output
	    mapper.map(key, value, output, reporter);
	    

	    if(incrProcCount) {
	        reporter.incrCounter(SkipBadRecords.COUNTER_GROUP, 
	        SkipBadRecords.COUNTER_MAP_PROCESSED_RECORDS, 1);
	    }
	}
      }
      
      else {
	  // added by Lijie Xu
	  int i = 0;
	  long record = 1;
	  int lcount = mapinrecordslimits.length;
	  // added end
	  
	  while (input.next(key, value)) {
		if(i < lcount && record++ == mapinrecordslimits[i]) 
		    Utils.heapdump(dumppath, "mapInRecords-" + mapinrecordslimits[i]
			    + "-out-" + reporter.getCounter(Task.Counter.MAP_OUTPUT_RECORDS).getValue());

	        // map pair to output
	        mapper.map(key, value, output, reporter);
	        if(incrProcCount) {
	          reporter.incrCounter(SkipBadRecords.COUNTER_GROUP, 
	              SkipBadRecords.COUNTER_MAP_PROCESSED_RECORDS, 1);
	        }
	  } 
      }
     
      // modified end
      
    } finally {
	if(MemoryMonitor.mapMonitorThread.isAlive())
	    MemoryMonitor.mapMonitorThread.interrupt();
	
      mapper.close();
    }
  }

  protected Mapper<K1, V1, K2, V2> getMapper() {
    return mapper;
  }
  

}
