package Assignment2.TimeComparision;
import org.apache.hadoop.io.*;

public class LatestDriverIpComparer extends WritableComparator {

	public LatestDriverIpComparer()
	{
		super(LatestDriverKeyPair.class,true);
	}
	
	public int compare(WritableComparable key1,WritableComparable key2)
	{
		LatestDriverKeyPair Key1=(LatestDriverKeyPair)key1;
		LatestDriverKeyPair Key2=(LatestDriverKeyPair)key2;
		return Key1.getIpAddress().compareTo(Key2.getIpAddress());
	}
	
}
