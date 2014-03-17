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

import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Shell;

/** Default {@link MapRunnable} implementation.*/
public class MapRunner<K1, V1, K2, V2>
    implements MapRunnable<K1, V1, K2, V2> {
  
  private Mapper<K1, V1, K2, V2> mapper;
  private boolean incrProcCount;
  
  // added by Lijie Xu
  private long mapinrecordslimit;
  private String dumppath;
  // added end
  
  @SuppressWarnings("unchecked")
  public void configure(JobConf job) {
    this.mapper = ReflectionUtils.newInstance(job.getMapperClass(), job);
    //increment processed counter only if skipping feature is enabled
    this.incrProcCount = SkipBadRecords.getMapperMaxSkipRecords(job)>0 && 
      SkipBadRecords.getAutoIncrMapperProcCount(job);
    
    // added by Lijie Xu
    mapinrecordslimit = job.getLong("heapdump.map.input.records", 0);
    String dumppath = job.get("heapdump.path", "/tmp");
    // added end
  }

  public void run(RecordReader<K1, V1> input, OutputCollector<K2, V2> output,
                  Reporter reporter)
    throws IOException {
    try {
      // allocate key & value instances that are re-used for all entries
      K1 key = input.createKey();
      V1 value = input.createValue();
      
      // added by Lijie Xu
      long i = 1;
      // added end
      
      // modified by Lijie Xu
      if(mapinrecordslimit == 0) {
	  while (input.next(key, value)) {
		  
	        // map pair to output
	        mapper.map(key, value, output, reporter);
	        if(incrProcCount) {
	          reporter.incrCounter(SkipBadRecords.COUNTER_GROUP, 
	              SkipBadRecords.COUNTER_MAP_PROCESSED_RECORDS, 1);
	        }
	  } 
      }
      else {
	  while (input.next(key, value)) {
		if(i++ == mapinrecordslimit) 
		    Utils.heapdump(dumppath, "mapInRecords-" + mapinrecordslimit);

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
      mapper.close();
    }
  }

  protected Mapper<K1, V1, K2, V2> getMapper() {
    return mapper;
  }
  

}
