package Assignment2.TimeComparision;

import org.apache.hadoop.io.*;

public class LatestAccessSortByDate extends WritableComparator{
 
	public LatestAccessSortByDate()
	{
		super(LatestDriverKeyPair.class,true);
	}
	
	public int compare(WritableComparable key1,WritableComparable key2)
	{
		LatestDriverKeyPair Key1=(LatestDriverKeyPair)key1;
		LatestDriverKeyPair Key2=(LatestDriverKeyPair)key2;
		int c=Key1.getIpAddress().compareTo(Key2.getIpAddress());
		if(c==0)
		{
			return -Key1.getDate().compareTo(Key2.getDate());
		}
		return c;
		
	}
}
