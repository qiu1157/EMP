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

		//必须设置Reduce任务数为1, 这样才能保证各Reduce是串行的
		job.setNumReduceTasks(1);
		//设置MAPPER输出格式
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
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
		HigherThanAveSalary htas = new HigherThanAveSalary();
		htas.run(args);
	}

	public static class MyMapper extends Mapper<Object, Text, IntWritable, Text> {

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] columns = value.toString().split(",");
			context.write(new IntWritable(0), new Text(columns[5]));
			context.write(new IntWritable(1), new Text(columns[1]+"+"+columns[5]));
		}
		
	}
	
	public static class MyReducer extends Reducer <IntWritable, Text, Text, Text> {
		private long allSalary = 0;
		private int allEmpCount = 0;
		private float aveSalary = 0;
		
		private long empSalary = 0;
		@Override
		protected void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text value : values) {
				if (0 == key.get()) {
					allSalary += Long.parseLong(value.toString());
					allEmpCount++ ;
				}else if(1 == key.get()) {
					aveSalary = allSalary/allEmpCount;
					empSalary = Long.parseLong(value.toString().split("\\+")[1]);
					if (empSalary > aveSalary) {
						context.write(new Text(value.toString().split("\\+")[0]), new Text(empSalary+""));
					}
				}
			}
		}
		
	}
}
