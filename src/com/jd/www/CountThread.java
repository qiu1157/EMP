package com.jd.www;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

@SuppressWarnings("rawtypes")
public class CountThread implements WritableComparable {
	Text threadId;
	IntWritable cnt;

	public CountThread() {
		// TODO Auto-generated constructor stub
		this.threadId = new Text();
		this.cnt = new IntWritable();
	}

	public CountThread(Text threadId, IntWritable cnt) {
		this.threadId = threadId;
		this.cnt = cnt;
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		// TODO Auto-generated method stub
		threadId.readFields(input);
		cnt.readFields(input);
	}

	@Override
	public void write(DataOutput output) throws IOException {
		// TODO Auto-generated method stub
		threadId.write(output);
		cnt.write(output);
	}
	@Override
	public int compareTo(Object object) {
		// TODO Auto-generated method stub
		return ((CountThread) object).cnt.compareTo(cnt) == 0 ? threadId.compareTo(((CountThread) object).threadId)
				: ((CountThread) object).cnt.compareTo(cnt);
	}

	
	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return this.threadId.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		// TODO Auto-generated method stub
		if ( ! (obj instanceof CountThread)) {
			return false ;
		}
		CountThread ct = (CountThread) obj;
		return threadId.equals(ct.threadId) && cnt.equals(ct.cnt);
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		StringBuffer buf = new StringBuffer();
		buf.append(this.threadId.toString());
		buf.append("\t");
		buf.append(this.cnt.toString());
		return buf.toString();
	}

	public Text getThreadId() {
		return threadId;
	}

	public void setThreadId(Text threadId) {
		this.threadId = threadId;
	}

	public IntWritable getCnt() {
		return cnt;
	}

	public void setCnt(IntWritable cnt) {
		this.cnt = cnt;
	}

}
