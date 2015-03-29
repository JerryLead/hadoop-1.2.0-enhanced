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
    


    public static boolean notified = false;

    private static long g = 0;
    private static long r = 0;

    private static Object toMonitor = new Object();
    private static Object finished = new Object();

    public static Thread mapMonitorThread = new Thread(new Runnable() {

	@Override
	public void run() {

	    while (true) {
		synchronized (toMonitor) {
		    try {
			toMonitor.wait();
		    } catch (InterruptedException e) {
			// TODO Auto-generated catch block
			// e.printStackTrace();
			return;
		    }

		    // lastRecord = r;
		    lastTotalKB = 0;
		    lastUsedKB = 0;

		    while (r % recordInterval == 1) {
			monitorMapMaxUsage(false);
		    }
		    monitorMapMaxUsage(true);

		}

		synchronized (finished) {
		    finished.notify();
		}
	    }

	}

    });

    public static Thread reduceMonitorThread = new Thread(new Runnable() {

	@Override
	public void run() {

	    while (true) {
		synchronized (toMonitor) {
		    try {
			toMonitor.wait();
		    } catch (InterruptedException e) {
			// TODO Auto-generated catch block
			// e.printStackTrace();
			return;
		    }

		    lastTotalKB = 0;
		    lastUsedKB = 0;

		    while (g % recordInterval == 1 && r % recordInterval == 1) {
			monitorReduceMaxUsage(false);
		    }
		    monitorReduceMaxUsage(true);

		}

		synchronized (finished) {
		    finished.notify();
		}
	    }

	}

    });


    public static void monitorMap() {

	long usedKB = (rt.totalMemory() - rt.freeMemory()) / 1024;

	System.err.println("record = " + r + ", total = " + rt.totalMemory()
		/ 1024 + ", used = " + usedKB);
    }

    public static void monitorReduce() {

	long usedKB = (rt.totalMemory() - rt.freeMemory()) / 1024;

	System.err.println("group = " + g + ", record = " + r + ", total = "
		+ rt.totalMemory() / 1024 + ", used = " + usedKB);
    }

    public static void monitorMapMaxUsage(boolean nextRecord) {

	long usedKB = (rt.totalMemory() - rt.freeMemory()) / 1024;
	long totalKB = (rt.totalMemory()) / 1024;

	if (usedKB > lastUsedKB) {
	    lastTotalKB = totalKB;
	    lastUsedKB = usedKB;
	}

	if (nextRecord)
	    System.err.println("record = " + lastRecord + ", total = "
		    + lastTotalKB + ", used = " + lastUsedKB + ", gcCount = "
		    + gcCount);

    }
    
    public static void monitorReduceMaxUsage(boolean nextRecord) {

	long usedKB = (rt.totalMemory() - rt.freeMemory()) / 1024;
	long totalKB = (rt.totalMemory()) / 1024;

	if (usedKB > lastUsedKB) {
	    lastTotalKB = totalKB;
	    lastUsedKB = usedKB;
	}

	if (nextRecord) {
	   
	   System.err.println("group = " + lastGroup + ", record = " + lastRecord + ", total = "
			    + lastTotalKB + ", used = " + lastUsedKB + ", gcCount = "
			    + gcCount);
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

	if (r % recordInterval == 1) {

	    try {
		gc();
		monitorMap();

		synchronized (toMonitor) {
		    lastRecord = r;
		    toMonitor.notify();
		    notified = true;
		}

	    } catch (InterruptedException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	    }
	}
    }

    public static void monitorBeforeReduceProcessRecord() {

	if (g % groupInterval == 1 && r % recordInterval == 1) {

	    try {
		gc();
		monitorReduce();

		synchronized (toMonitor) {
		    lastGroup = g;
		    lastRecord = r;
		    toMonitor.notify();
		    notified = true;
		}

	    } catch (InterruptedException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	    }
	}
    }

    public static void monitorAfterProcessRecord() {
	if (notified == true) {
	    synchronized (finished) {
		try {
		    finished.wait();
		    notified = false;
		} catch (InterruptedException e) {
		    // TODO Auto-generated catch block
		    e.printStackTrace();
		}

	    }
	}
    }

    public static void addRecord() {
	++r;
    }

    public static void addGroup() {
	++g;
	r = 0;
    }


   
}
