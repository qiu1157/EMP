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

public class Recommenddation {

	public void run(String args[]) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "JobName");
		job.setJarByClass(com.jd.www.Recommenddation.class);
		job.setMapperClass(PostMapper.class);

		job.setReducerClass(PostReducer.class);

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

	}
	
	public static class PostMapper extends Mapper<Object, Text, Text, Text> {

		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String currThreadId = context.getConfiguration().get("currThreadId");
			String[] columns = value.toString().split(",");
			
			if(columns[0].equals(currThreadId)) {
				context.write(new Text(columns[1]), new Text("INEEDT"));
			}
			context.write(new Text(columns[1]), new Text(columns[0]));
		}
		
	}

	public static class PostReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
		}
		
	}
}
