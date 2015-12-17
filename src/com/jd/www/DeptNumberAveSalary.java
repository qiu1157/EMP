package com.jd.www;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
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

public class DeptNumberAveSalary {

	public void run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "DeptNumberAveSalary");
		job.setJarByClass(com.jd.www.DeptNumberAveSalary.class);
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
		DeptNumberAveSalary snas = new DeptNumberAveSalary();
		snas.run(args);
	}

	public static class MyMapper extends Mapper<Object, Text, Text, Text> {
		Map<String, String> deptMap = new HashMap<String, String>();

		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] columns = value.toString().split(",");
			if(null != columns) {
				if (deptMap.containsKey(columns[7])) {
					context.write(new Text(deptMap.get(columns[7].trim())), new Text(columns[5].trim()));
				}
			}
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.setup(context);
			BufferedReader in = null;
			String line = null;
			Path[] paths = context.getLocalCacheFiles();
			try {
				if (null != paths) {
					for (Path path : paths) {
						if (path.toString().contains("dept")) {
							in = new BufferedReader(new FileReader(path.toString()));
							while (null != (line = in.readLine())) {
								deptMap.put(line.split(",")[0], line.split(",")[1]);
							}
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
			int count = 0;
			int sumSalary = 0;
			for(Text value : values) {
				count++;
				sumSalary += Integer.parseInt(value.toString());
			}
			float avgSalary = sumSalary/count;
			context.write(key, new Text("emps:="+count+"  avgSalary:="+avgSalary));
		}
	}
}
