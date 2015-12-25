package com.jd.www;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class EmpSalarySort {

	public void run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "JobName");
		job.setJarByClass(com.jd.www.EmpSalarySort.class);
		job.setMapperClass(MyMapper.class);

		job.setReducerClass(MyReducer.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path("hdfs://master.hadoop:9000/in/emp"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://master.hadoop:9000/out"));

		if (!job.waitForCompletion(true))
			return;		
	}
	public static void main(String[] args) throws Exception {
		EmpSalarySort ess = new EmpSalarySort();
		ess.run(args);
	}

	public static class MyMapper extends Mapper<Object, Text, IntWritable, Text> {

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] columns = value.toString().split(",");
			
			if (null != columns && columns.length > 1) {
				System.out.println("columns.length==" +columns.length);
				long salary = Long.parseLong("".equals(columns[5]) ? "0" : columns[5]);
				long comm = Long.parseLong("".equals(columns[6])? "0" : columns[6]);
				context.write(new IntWritable(1), new Text(columns[1] + "+" +(salary+comm)));
			}
		}
		
	}
	
	public static class MyReducer extends Reducer<IntWritable, Text, Text, LongWritable> {

		@Override
		protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Map<String, Long> empSalary = new TreeMap<String, Long>();
			for (Text value : values) {
				String[] columns = value.toString().split("\\+");
				empSalary.put(columns[0], Long.parseLong(columns[1]));
			}
			List<Map.Entry<String, Long>> list = new ArrayList<Map.Entry<String, Long>>(empSalary.entrySet());
			Collections.sort(list, new Comparator<Map.Entry<String, Long>>(){

				@Override
				public int compare(Entry<String, Long> arg0, Entry<String, Long> arg1) {
					// TODO Auto-generated method stub
					return arg1.getValue().compareTo(arg0.getValue());
				}
				
			});
			for (Map.Entry<String, Long> map : list) {
				context.write(new Text(map.getKey()), new LongWritable(map.getValue()));
			}
		}
		
	}
}
