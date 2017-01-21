package Assignment2.TimeComparision;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

public class LatestAccessDriver extends Configured implements Tool {

	public static void main(String [] args) throws Exception
	{
		int exitCode=ToolRunner.run(new Configuration(), new LatestAccessDriver(), args);
		System.exit(exitCode);
	}

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		@SuppressWarnings("deprecation")
		Job jobTracker=new Job(getConf());
		jobTracker.setJarByClass(LatestAccessDriver.class);
		jobTracker.setJobName("LatestAccess");
		
		FileInputFormat.addInputPath(jobTracker, new Path(args[0]));
		FileOutputFormat.setOutputPath(jobTracker,new Path(args[1]));
		jobTracker.setMapperClass(LatestAccessMapper.class);
		jobTracker.setMapOutputKeyClass(LatestDriverKeyPair.class);
		jobTracker.setMapOutputValueClass(Text.class);
		jobTracker.setReducerClass(LatestAccessReducer.class);
		jobTracker.setGroupingComparatorClass(LatestDriverIpComparer.class);
		jobTracker.setSortComparatorClass(LatestAccessSortByDate.class);
		jobTracker.setPartitionerClass(LatestAccessPartitioner.class);
		jobTracker.setOutputKeyClass(Text.class);
		jobTracker.setOutputValueClass(Text.class);

		return (jobTracker.waitForCompletion(true)?0:1);
	}
	
	
	
}
