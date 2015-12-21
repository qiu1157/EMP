package com.jd.www;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DeptEarliestEmp {

	public void run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "DeptEarliestEmp");
		job.setJarByClass(com.jd.www.DeptEarliestEmp.class);
		job.setMapperClass(MyMapper.class);

		job.setReducerClass(MyReducer.class);

		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.addCacheFile(new Path("/in/dept").toUri());
		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path("/in/emp"));
		FileOutputFormat.setOutputPath(job, new Path("/out"));

		if (!job.waitForCompletion(true))
			return;
	}

	public static void main(String[] args) throws Exception {
		DeptEarliestEmp dee = new DeptEarliestEmp();
		dee.run(args);
	}

	public static class MyMapper extends Mapper<Object, Text, Text, Text> {
		Map<String, String> deptMap = new HashMap<String, String>();

	public static class MyMapper extends Mapper<Object, Text, Text, Text> {
		Map<String, String> deptMap = new HashMap<String, String>();

		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] columns = value.toString().split(",");
			if (null == columns) {
				context.getCounter("USERGROUPMAP", "columns is null").increment(1);
				return;
			}

			if (deptMap.containsKey(columns[7])) {
				context.write(new Text(deptMap.get(columns[7])), new Text(columns[1] + "+" + columns[4]));
			}
		}

		@SuppressWarnings("deprecation")
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			BufferedReader in = null;
			String line = null;
			Path[] paths = context.getLocalCacheFiles();
			try {
				for (Path path : paths) {
					if (path.toString().contains("dept")) {
						in = new BufferedReader(new FileReader(path.toString()));
						while (null != (line = in.readLine())) {
							deptMap.put(line.split(",")[0], line.split(",")[1]);
						}
					}
				}
			} finally {
				// TODO: handle finally clause
				if (null != in) {
					in.close();
				}
			}
		}

	}

	public static class MyReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			DateFormat sdf = new SimpleDateFormat("dd-MM月-yy");
			Date empEntryDate = null;
			Date earliestDate = new Date();
			String earliestEMP = null;
			for (Text value : values) {
				String[] str = value.toString().split("\\+");
				try {
					empEntryDate = sdf.parse(str[1]);
					if (empEntryDate.compareTo(earliestDate) < 0) {
						earliestDate = empEntryDate;
						earliestEMP = str[0];
					}
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			context.write(key, new Text(earliestEMP+"\t"+sdf.format(earliestDate)));
		}

	}
}
