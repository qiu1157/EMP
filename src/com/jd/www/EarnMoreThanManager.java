package com.jd.www;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class EarnMoreThanManager {

	public void run(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "EarnMoreThanManager");
		job.setJarByClass(com.jd.www.EarnMoreThanManager.class);
		job.setMapperClass(MyMapper.class);

		job.setReducerClass(MyReducer.class);

		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path("hdfs://master.hadoop:9000/in/emp"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://master.hadoop:9000/out"));

		if (!job.waitForCompletion(true))
			return;		
	}
	public static void main(String[] args) throws Exception {
		EarnMoreThanManager emtm = new EarnMoreThanManager();
		emtm.run(args);
	}

	public static class MyMapper extends Mapper<Object, Text, Text, Text> {

		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] columns = value.toString().split(",");
			if (null != columns || columns.length > 0) {
				String id = columns[0];
				String pid = columns[3];
				String empName = columns[1];
				String salary = columns[5];
				context.write(new Text(pid), new Text("T1+"+empName+"+"+salary));
				context.write(new Text(id), new Text("T2+"));
			}
			
		}
	}
	
	public static class MyReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub

		}
	}
}
