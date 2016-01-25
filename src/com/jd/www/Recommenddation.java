package com.jd.www;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Recommenddation {

	public int run(String args[])
			throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		String pfix="hdfs://192.168.181.128:9000";
		Path postJobInput = new Path(pfix+"/in2");
		Path postJobOutput = new Path(pfix+"/out/post");
		
		Path countJobInput = new Path(pfix+"/out/post");
		Path countJobOutput = new Path(pfix+"/out/count");
		
		Path sortJobInput = new Path(pfix+"/out/count");
		Path sortJobOutput = new Path(pfix+"/out/sort");
		
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
		job2.setMapperClass(CountMapper.class);
		job2.setReducerClass(CountReducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job2, countJobInput);
		FileOutputFormat.setOutputPath(job2, countJobOutput);
		
		Job job3 = Job.getInstance(conf, "Job3:sort");
		job3.setMapperClass(SortMapper.class);
		job3.setReducerClass(SortReducer.class);
		job3.setMapOutputKeyClass(CountThread.class);
		job3.setMapOutputValueClass(Text.class);
		job3.setOutputKeyClass(CountThread.class);
		job3.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job3, sortJobInput);
		FileOutputFormat.setOutputPath(job3, sortJobOutput);
		
		ControlledJob cjob1 = new ControlledJob(job1.getConfiguration());
		ControlledJob cjob2 = new ControlledJob(job2.getConfiguration());
		ControlledJob cjob3 = new ControlledJob(job3.getConfiguration());
		
		cjob2.addDependingJob(cjob1);
		cjob3.addDependingJob(cjob2);
		
		JobControl jobControl = new JobControl("Recommenddation");
		jobControl.addJob(cjob1);
		jobControl.addJob(cjob2);
		jobControl.addJob(cjob3);
		
		cjob1.setJob(job1);
		cjob2.setJob(job2);
		cjob3.setJob(job3);
		
		Thread thread = new Thread(jobControl);
		thread.start();
		while (! jobControl.allFinished()) {
			Thread.sleep(500);
		}
		jobControl.stop();
		return 0 ;
	}

	public static void main(String[] args) throws Exception {
		Recommenddation r = new Recommenddation();
		r.run(args);
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
			Set<Text> set = new HashSet<Text>();
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
			System.out.println("key=="+key);
			String[] columns = value.toString().split("\t");
			context.write(new Text(columns[0]), new Text(columns[1]));
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

	public static class SortMapper extends Mapper<Object, Text, CountThread, Text> {

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			CountThread ct = new CountThread();
			String[] columns = value.toString().split("\t");
			ct.setThreadId(new Text(columns[0]));
			ct.setCnt(new IntWritable(Integer.parseInt(columns[1])));
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
