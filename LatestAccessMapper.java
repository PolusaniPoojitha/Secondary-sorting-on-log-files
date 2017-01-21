package Assignment2.TimeComparision;

import org.apache.hadoop.mapreduce.*;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.net.ntp.TimeStamp;
import org.apache.hadoop.io.*;

public class LatestAccessMapper extends Mapper<LongWritable, Text,LatestDriverKeyPair,Text> {
	
	public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException 
	{
		String readLine=value.toString();
		int DateTimeOffsetBegin=readLine.indexOf('[');
		int DateTimeOffsetEnd=readLine.indexOf(']');
		if(DateTimeOffsetBegin!=-1 && DateTimeOffsetEnd!=-1)
		{
			String Date=readLine.substring(DateTimeOffsetBegin+1, DateTimeOffsetEnd);
			String DateValue=readLine.substring(DateTimeOffsetBegin,DateTimeOffsetEnd+1);
			String IpAddress=readLine.split(" ")[0];
			SimpleDateFormat format=new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss Z");
			Date parsedDate1;
			try {
				parsedDate1 = format.parse(Date);
				TimeStamp time=new TimeStamp(parsedDate1);
				LatestDriverKeyPair obj=new LatestDriverKeyPair(IpAddress,time.getTime());
				context.write(obj,new Text(DateValue));	
			}
			catch(Exception ex)
			{}
		}
	
	}
}
