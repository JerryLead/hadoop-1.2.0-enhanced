package org.apache.hadoop.mapred;

public class MemoryMonitor {

    private static Runtime rt = Runtime.getRuntime();

    public static long groupInterval = 0;
    public static long recordInterval = 0;
    
    private static long lastGroup = 1;
    private static long lastRecord = 1;
    private static long lastTotalKB = 0;
    private static long lastUsedKB = 0;
    private static int gcCount = 0;

    public static boolean toMonitor = false;
    public static boolean performGC = false;

    private static long g = 0;
    private static long r = 0;
    
    private static Object recordLock = new Object();
    
    public static Thread mapMonitorThread = new Thread(new Runnable() {

	@Override
	public void run() {
	    while (!Thread.currentThread().isInterrupted()) {

		if (toMonitor)
		    monitorMapMaxUsage();
		else
		    performGC = false;
	    }
	}

    });
    
    public static Thread reduceMonitorThread = new Thread(new Runnable() {

	@Override
	public void run() {
	    while (!Thread.currentThread().isInterrupted()) {

		if (toMonitor)
		    mointorReduceMaxUsage();
		else
		    performGC = false;
	    }
	}

    });
    
    public static void monitorMap() {

	long usedKB = (rt.totalMemory() - rt.freeMemory()) / 1024;

	System.err.println("record = " + r + ", total = "
		+ rt.totalMemory() / 1024 + ", used = " + usedKB);
    }

    public static void monitorReduce() {

	long usedKB = (rt.totalMemory() - rt.freeMemory()) / 1024;

	System.err.println("group = " + g + ", record = " + r
			+ ", total = " + rt.totalMemory() / 1024 + ", used = "
			+ usedKB);
    }



    public static void monitorMapMaxUsage() {
	
	synchronized (recordLock) {
	    
	    if (toMonitor == true) {
		long usedKB = (rt.totalMemory() - rt.freeMemory()) / 1024;
		long totalKB = (rt.totalMemory()) / 1024;
		
		if (r == lastRecord) {
		    if (usedKB > lastUsedKB) {
			lastTotalKB = totalKB;
			lastUsedKB = usedKB;
		    }
		}

		else {
		    System.err.println("record = " + lastRecord + ", total = " + lastTotalKB
			    + ", used = " + lastUsedKB + ", gcCount = " + gcCount);
		    lastRecord = r;
		    lastTotalKB = totalKB;
		    lastUsedKB = usedKB;
		}
	    }
	    
	}
	
    }

    public static void mointorReduceMaxUsage() {
	synchronized (recordLock) {
	    if (toMonitor == true) {
		long usedKB = (rt.totalMemory() - rt.freeMemory()) / 1024;
		long totalKB = (rt.totalMemory()) / 1024;

		if (g == lastGroup && r == lastRecord) {
		    if (usedKB > lastUsedKB) {
			lastTotalKB = totalKB;
			lastUsedKB = usedKB;
		    }	
		} else {
		    System.err.println("group = " + lastGroup + ", record = " + lastRecord
			    + ", total = " + lastTotalKB + ", used = " + lastUsedKB
			    + ", gcCount = " + gcCount);

		    lastGroup = g;
		    lastRecord = r;
		    lastTotalKB = totalKB;
		    lastUsedKB = usedKB;
		}
	    }
	}
    }

    public static void gc() throws InterruptedException {
	System.gc();
	System.runFinalization();

	++gcCount;
    }

    public static void gcWithSleep() throws InterruptedException {
	System.gc();
	System.runFinalization();
	Thread.sleep(100);

	System.gc();
	System.runFinalization();
	Thread.sleep(100);
    }
    
    public static void monitorBeforeMapProcessRecord() {
	try {
	    if (performGC)
		gc();
  
	    if (r % recordInterval == 1) {

		try {
		    gc();
		    monitorMap();
		    
		} catch (InterruptedException e) {
		    // TODO Auto-generated catch block
		    e.printStackTrace();
		}

		toMonitor = true;
		performGC = true;

	    }

	    // Thread.sleep(10);
	} catch (InterruptedException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
   	
    }

    public static void monitorBeforeReduceProcessRecord() {
	try {
	    if (performGC)
		gc();
	    
	    if (g % groupInterval == 1  && r % recordInterval == 1) {

		try {
		    gc();
		    monitorReduce();
		    
		} catch (InterruptedException e) {
		    // TODO Auto-generated catch block
		    e.printStackTrace();
		}

		toMonitor = true;
		performGC = true;

	    }

	    // Thread.sleep(10);
	} catch (InterruptedException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
    }

    public static void monitorAfterProcessRecord() {
	toMonitor = false;
    }

    public static void addRecord() {
	synchronized (recordLock) {
	    ++r;
	}
    }
    
    public static void addGroup() {
	++g;
	
	toMonitor = false;
	r = 0;
	
    }

   
}
