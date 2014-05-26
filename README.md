# Enhanced Hadoop-1.2.0

Compared with original hadoop-1.2.0, this version can

1. **Output detailed logs. The detailed logs  contain:** 

	- concrete processing steps
	- Volume of input and output data after user code finishes
	- Current dataflow counters when user code starts
	
	For example,
		
		Mapper 
		￼22:57:44 INFO [map() starts]

2. **Display real-time memory usage in task's JVM.**

	Users only need to specify a configuration 'child.monitor.jstat.seconds = 2'
	
	For example, the real-time memory usage of a reducer is as follows.
	

	**Eden space**
![EdenSpace](https://raw.githubusercontent.com/JerryLead/OOMCases/master/Count-distinct-reducer/first-oom-job/logs/taskPerfGraph.png)
	**New generation**
![NewGen](https://raw.githubusercontent.com/JerryLead/OOMCases/master/Count-distinct-reducer/first-oom-job/logs/taskPerfGraph%20-1.png)
	**Old generation**
![OldGen](https://raw.githubusercontent.com/JerryLead/OOMCases/master/Count-distinct-reducer/first-oom-job/logs/taskPerfGraph%20-2.png)
	**GC counts**
![GC counts](https://raw.githubusercontent.com/JerryLead/OOMCases/master/Count-distinct-reducer/first-oom-job/logs/taskPerfGraph%20-3.png)
3. **Generate 'heap dump@record(i)' if one or some following configurations are specified.**

	Heap dump configurations.

	| Configuration | Description| The modified code |
	|:--------------------|:-----------------|------------------:|
	| heapdump.path | where the dump will be generated (by default,  /tmp) | |
	| heapdump.map.file.bytes.read | external bytes read | Task.java |
	| heapdump.map.input.records | map input records | Mapper.java & MapRunner.java |
	| heapdump.map.combine.input.records | combine input records in mapper | ReduceContext.java & Task.java | 
	| heapdump.reduce.combine.input.records | combine input records in reducer | ReduceContext.java  & Task.java |
	| heapdump.reduce.input.group |reduce input groups | Reducer.java & ReduceTask.java |
	| heapdump.reduce.input.records | reduce input records| ReduceTask.java & ReduceContext.java | 
	| heapdump.reduce.file.bytes.read |external bytes read | Task.java | 
	| task.reporter.update.ms | 3000 | Task.java |

	For example, if users specify
	`conf.set("heapdump.map.input.records", "9425[5]")`

	Mapper will dump the heap when it encounter the i-th specified record (1885th, 3769th, 5653rd, 7537th, 9425th)

		2014-04-27 21:03:38,797 INFO org.apache.hadoop.mapreduce.Mapper: [map() begins]
		2014-04-27 21:03:54,368 INFO org.apache.hadoop.mapred.Utils: [HeapDump] generate /tmp/mapInRecords-1885-out-0-pid-26746.hprof
		2014-04-27 21:04:10,118 INFO org.apache.hadoop.mapred.Utils: [HeapDump] generate /tmp/mapInRecords-3769-out-0-pid-26746.hprof
		2014-04-27 21:04:33,437 INFO org.apache.hadoop.mapred.Utils: [HeapDump] generate /tmp/mapInRecords-5653-out-0-pid-26746.hprof
		2014-04-27 21:04:57,338 INFO org.apache.hadoop.mapred.Utils: [HeapDump] generate /tmp/mapInRecords-7537-out-0-pid-26746.hprof
		2014-04-27 21:05:08,634 INFO org.apache.hadoop.mapred.Utils: [HeapDump] generate /tmp/mapInRecords-9425-out-0-pid-26746.hprof
