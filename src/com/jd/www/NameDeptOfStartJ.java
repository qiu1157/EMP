package com.jd.www;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NameDeptOfStartJ {
	public void run(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException  {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "NameDeptOfStartJ");
		job.setJarByClass(com.jd.www.NameDeptOfStartJ.class);
		job.setMapperClass(MyMapper.class);

		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//加载缓存文件
		job.addCacheFile(new Path("hdfs://master.hadoop:9000/in/dept").toUri());
		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path("hdfs://master.hadoop:9000/in/emp"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://master.hadoop:9000/out"));

		if (!job.waitForCompletion(true))
			return;
	}
	public static void main(String[] args) throws Exception {
		NameDeptOfStartJ ndo = new NameDeptOfStartJ();
		ndo.run(args);
	}
	
	public static class MyMapper extends Mapper<Object, Text, Text, Text>	 {
		Map<String, String>	deptMap = new HashMap<String, String>();
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] columns = value.toString().split(",");
			if (null != columns || columns.length > 1 ) {
				if (deptMap.containsKey(columns[7]) && columns[1].toUpperCase().startsWith("J")) {
					context.write(new Text(deptMap.get(columns[7])), new Text(columns[1]));
				}			
			}
		}

		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			BufferedReader in = null;
			String line = null;
			// TODO Auto-generated method stub
			super.setup(context);
			try {
				Path[] paths = context.getLocalCacheFiles();
				for (Path path : paths) {
					System.out.println("uri path==" + path.toString());
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
	
}
