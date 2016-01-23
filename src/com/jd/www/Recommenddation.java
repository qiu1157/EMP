package com.jd.www;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Recommenddation {

	public void run(String args[])
			throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Path postJobInput = new Path("");
		Path postJobOutput = new Path("");
		Path countJobInput = new Path("");
		Path countJobOutput = new Path("");
		Path sortJobInput = new Path("");
		Path sortJobOutput = new Path("");
		
		Job job1 = Job.getInstance(conf, "Job1: Post");
		job1.setJarByClass(com.jd.www.Recommenddation.class);
		job1.setMapperClass(PostMapper.class);
		job1.setReducerClass(PostReducer.class);
		// TODO: specify output types
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job1, postJobInput);
		FileOutputFormat.setOutputPath(job1, postJobOutput);

		Job job2 = Job.getInstance(conf, "Job2:Count");
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

			if (columns[0].equals(currThreadId)) {
				context.write(new Text(columns[1]), new Text("INEEDT"));
			}
			context.write(new Text(columns[1]), new Text(columns[0]));
		}

	}

	public static class PostReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			boolean find = false;
			Set<Text> set = new HashSet();
			for (Text value : values) {
				if ("INEEDT".equals(value.toString())) {
					find = true;
				} else {
					set.add(new Text(value.toString()));
				}
			}
			if (find) {
				Iterator<Text> it = set.iterator();
				while (it.hasNext()) {
					context.write(it.next(), new Text("1"));
				}
			}
		}

	}

	public static class CountMapper extends Mapper<Object, Text, Text, Text> {

		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			context.write((Text) key, value);
		}

	}

	public static class CountReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int count = 0;
			for (Text val : values) {
				count++;
			}
			context.write(key, new Text(count + ""));
		}

	}

	public static class SortMapper extends Mapper<Text, Text, CountThread, Text> {

		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, CountThread, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			CountThread ct = new CountThread();
			ct.setThreadId(key);
			ct.setCnt(new IntWritable(Integer.parseInt(value.toString())));
			context.write(ct, new Text());
		}

	}

	public static class SortReducer extends Reducer<CountThread, Text, CountThread, Text> {

		@Override
		protected void reduce(CountThread key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			context.write(key, null);
		}

	}
}
