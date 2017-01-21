package Assignment2.TimeComparision;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.*;

public class LatestDriverKeyPair implements WritableComparable<LatestDriverKeyPair> {

	private Text IpAddress;
	private LongWritable _Date;
	public LatestDriverKeyPair()
	{
		IpAddress=new Text();
		_Date=new LongWritable();
	}
	public LatestDriverKeyPair(String ipAddress,long date)
	{
		IpAddress=new Text(ipAddress);
		_Date=new LongWritable(date);
	}
	public Text getIpAddress()
	{
		return IpAddress;
	}
	public LongWritable getDate()
	{
		return _Date;
	}
	public void setIpAddress(Text _ipAddress)
	{
		this.IpAddress=_ipAddress;
	}
	public void setDate(LongWritable _date)
	{
		this._Date=_date;
	}
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		IpAddress.write(out);
		_Date.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		IpAddress.readFields(in);
		_Date.readFields(in);
	}
	public int compareTo(LatestDriverKeyPair o) {
		// TODO Auto-generated method stub
		return 0;
	}

}
