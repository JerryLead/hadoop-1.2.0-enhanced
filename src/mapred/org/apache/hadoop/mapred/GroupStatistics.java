package org.apache.hadoop.mapred;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class GroupStatistics {

    private static int[] recordsInGroup = new int[100000];
    private static int storedGroups = 0;

    private static long lastRecordCounter = 0;

    public static void record(long currentGroupCounter, long currentRecordCounter) {

	if (currentGroupCounter > 0) {
	    if (storedGroups < 100000) {
		recordsInGroup[storedGroups] = (int) (currentRecordCounter - lastRecordCounter);
		++storedGroups;
		lastRecordCounter = currentRecordCounter;
	    }
	}

    }

    public static void printGroupStatistics() {
	List<Integer> statistics = new ArrayList<Integer>();

	long sum = 0;

	for (int i = 0; i < storedGroups; i++) {
	    statistics.add(recordsInGroup[i]);
	    sum += recordsInGroup[i];
	}

	if (storedGroups > 0) {
	    Collections.sort(statistics);

	    System.out.println("Min = " + statistics.get(0));
	    System.out.println("Max = " + statistics.get(storedGroups - 1));
	    System.out.println("Mean = " + sum / storedGroups);
	    System.out.println("Median = " + statistics.get(storedGroups / 2));

	    if (storedGroups >= 4) {
		System.out.println("Q1 = " + statistics.get(storedGroups / 4));
		System.out.println("Q3 = "
			+ statistics.get(storedGroups - storedGroups / 4 - 1));
	    }
	}
    }
}
