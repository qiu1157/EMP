package com.jd.www;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.jd.www.EmpSalarySort2.MyMapper.DecreaseComparator;

public class EmpSalarySort2 {

	public void run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "EmpSalarySort2");
		job.setJarByClass(com.jd.www.EmpSalarySort2.class);
		job.setMapperClass(MyMapper.class);

		
		// TODO: specify output types
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		//设置map端的排序
		job.setSortComparatorClass(DecreaseComparator.class);
		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path("hdfs://master.hadoop:9000/in/emp"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://master.hadoop:9000/out"));

		if (!job.waitForCompletion(true))
			return;		
	}
	public static void main(String[] args) throws Exception {
		EmpSalarySort2 ess2 = new EmpSalarySort2();
		ess2.run(args);
	}

	public static class MyMapper extends Mapper<Object, Text, LongWritable, Text> {

		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, LongWritable, Text>.Context context)
				throws IOException, InterruptedException {
			String[] columns = value.toString().split(",");
			if (null != columns && columns.length > 1) {
				System.out.println("columns[5]=="+columns[5]);
				long empSalary = Long.parseLong("".equals(columns[5]) ? "0" : columns[5]);
				long comm = Long.parseLong("".equals(columns[6]) ? "0" : columns[6]);
				context.write(new LongWritable(empSalary+comm), new Text(columns[1]));
			}	
		}
		
		public static class DecreaseComparator extends LongWritable.Comparator {

			@Override
			public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
				// TODO Auto-generated method stub
				return -super.compare(b1, s1, l1, b2, s2, l2);
			}

			@Override
			public int compare(WritableComparable a, WritableComparable b) {
				// TODO Auto-generated method stub
				return -super.compare(a, b);
			}
			
		}
	}
	
}
