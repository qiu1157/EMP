/**    
* @Title: SumDeptSalary.java  
* @Package com.jd.www  
* @Description: TODO(用一句话描述该文件做什么)  
* @author A18ccms A18ccms_gmail_com    
* @date 2015年12月12日 下午4:01:38  
* @version V1.0    
*/

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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 *  
 * 
 * @ClassName: SumDeptSalary 
 * @Description: TODO(这里用一句话描述这个类的作用) 
 * @author A18ccms a18ccms_gmail_com 
 * @date 2015年12月12日 下午4:01:38     
 */

public class SumDeptSalary {

	public void run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		Job job = Job.getInstance(conf, "SumDeptSalary");
		job.setJarByClass(com.jd.www.SumDeptSalary.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
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

		/*
		 * (非 Javadoc)  <p>Title: map</p>  <p>Description: </p> 
		 * 
		 * @param key
		 * 
		 * @param value
		 * 
		 * @param context
		 * 
		 * @throws IOException
		 * 
		 * @throws InterruptedException 
		 * 
		 * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object,
		 * java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context) 
		 */

		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] columns = value.toString().split(",");
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String filename = fileSplit.getPath().getName();
			if ("dept".equals(filename)) {
				if (columns.length == 3) {
					context.write(new Text(columns[0]), new Text("dept+" + columns[1]));
				}
			} else if ("emp".equals(filename)) {
				if (columns.length == 8) {
					context.write(new Text(columns[7]), new Text("emp+" + columns[5]));
				}
			}
		}

		/*
		 * (非 Javadoc)  <p>Title: setup</p>  <p>Description: </p> 
		 * 
		 * @param context
		 * 
		 * @throws IOException
		 * 
		 * @throws InterruptedException 
		 * 
		 * @see
		 * org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.
		 * Mapper.Context) 
		 */

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.setup(context);
		}
	}

	public static class MyReducer extends Reducer<Text, Text, Text, Text> {

		/*
		 * (非 Javadoc)  <p>Title: reduce</p>  <p>Description: </p> 
		 * 
		 * @param arg0
		 * 
		 * @param arg1
		 * 
		 * @param arg2
		 * 
		 * @throws IOException
		 * 
		 * @throws InterruptedException 
		 * 
		 * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object,
		 * java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context) 
		 */

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Text textKey = new Text();
			Text intValue = new Text();
			int sum = 0;
			for (Text val : values) {
				String[] tmps = val.toString().split("\\+");
				context.getCounter("USEGROUPREDUCER", val.toString()).increment(1);
				if (tmps[0].equals("dept")) {
					textKey.set(tmps[1]);
				} else if (tmps[0].equals("emp")) {
					sum += Integer.parseInt(tmps[1]);
				}
			}
			intValue.set(sum + "");
			context.write(textKey, intValue);
		}
	}

	public static void main(String[] args) throws Exception {
		SumDeptSalary sds = new SumDeptSalary();
		sds.run(args);
	}

}
