package Assignment2.TimeComparision;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;


public class LatestAccessPartitioner  extends Partitioner<LatestDriverKeyPair,Text> {
	@Override
	public int getPartition(LatestDriverKeyPair key, Text value, int numPartitions) {
		// TODO Auto-generated method stub
		return Math.abs(key.getIpAddress().hashCode()%numPartitions);
	}
}
