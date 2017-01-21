package Assignment2.TimeComparision;

import org.apache.hadoop.mapreduce.*;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.*;

public class LatestAccessReducer extends Reducer<LatestDriverKeyPair, Text, Text, Text> {
	 public void reduce(LatestDriverKeyPair key, Iterable<Text> values, Context context)
		      throws IOException, InterruptedException {
		 int count=-1;
		 for(Text value:values)
		 {
			 count++;
			 context.write(key.getIpAddress(),value);
			if(count==2)break;
		 }
	 }
}
