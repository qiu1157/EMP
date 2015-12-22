package com.jd.www;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HigherThanAveSalary {
	public  void run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "HigherThanAveSalary");
		job.setJarByClass(com.jd.www.HigherThanAveSalary.class);
		job.setMapperClass(MyMapper.class);

		job.setReducerClass(MyReducer.class);

		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path("src"));
		FileOutputFormat.setOutputPath(job, new Path("out"));

		if (!job.waitForCompletion(true))
			return;		
	}
	public static void main(String[] args) throws Exception {
		HigherThanAveSalary htas = new HigherThanAveSalary();
		htas.run(args);
	}

	public static class MyMapper extends Mapper<Object, Text, IntWritable, Text> {

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] columns = value.toString().split(",");
			context.write(new IntWritable(0), value);
		}
		
	}
	
	public static class MyReducer extends Reducer <IntWritable, Text, Text, Text> {

		@Override
		protected void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
		}
		
	}
}
