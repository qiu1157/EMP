package com.jd.www;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SumCitySalary {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "SumCitySalary");
		job.setJarByClass(com.jd.www.SumCitySalary.class);
		
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);

		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path("hdfs://master.hadoop:9000/in"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://master.hadoop:9000/out"));

		if (!job.waitForCompletion(true))
			return;
	}

	public static class MyMapper extends Mapper<Object, Text, Text, Text> {

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String fileName = fileSplit.getPath().getName();
			System.out.println("fileName=="+fileName);
			String[] columns = value.toString().split(",");
			if ( null != columns || columns.length > 0 ) {
				if ("dept".equals(fileName)) {
					context.write(new Text(columns[0]), new Text("dept+" + columns[2]));
				} else if ("emp".equals(fileName)) {
					context.write(new Text(columns[7]), new Text("emp+" + columns[5]));
				} 
			}
		}
	}
	
	public static class MyReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Text cityName = null;
			int sumSalary = 0;
			for (Text value : values) {
				String[] columns = value.toString().split("\\+");
				if ("dept".equals(columns[0])) {
					cityName = new Text(columns[1]);
				}else if ("emp".equals(columns[0])) {
					sumSalary += Integer.parseInt(columns[1]);
				}
			}
			context.write(cityName, new Text(sumSalary+""));
		}
		
	}
}
