/**    
* @Title: MiddlePersonsCountForComm.java  
* @Package com.jd.www  
* @Description: TODO(用一句话描述该文件做什么)  
* @author qiuxiangu@gmail.com    
* @date 2016年1月2日 下午6:20:33  
* @version V1.0    
*/

package com.jd.www;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *  
 * 
 * @ClassName: MiddlePersonsCountForComm 
 * @Description: TODO(这里用一句话描述这个类的作用) 
 * @author qiuxiangu@jd.com
 * @date 2016年1月2日 下午6:20:33     
 */

public class MiddlePersonsCountForComm {
	public void run(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "MiddlePersonsCountForComm");
		job.setJarByClass(com.jd.www.MiddlePersonsCountForComm.class);
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
		MiddlePersonsCountForComm mpcfc = new MiddlePersonsCountForComm();
		mpcfc.run(args);
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
			context.write(new Text("0"), new Text(columns[0] + "," + ("".equals(columns[3]) ? " " : columns[3])));
		}

	}

	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
		List<String> employeeList = new ArrayList<String>();
		Map<String, String> employeeToManagerMap = new HashMap<String, String>();
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

			for (Text value : values) {
				employeeList.add(value.toString().split(",")[0]);
				employeeToManagerMap.put(value.toString().split(",")[0].trim(), value.toString().split(",")[1].trim());
			}

			int totalEmployee = employeeList.size();
			int i, j;
			int distance;
			for (i = 0; i < totalEmployee - 1; i++) {
				for (j = (i + 1); j < totalEmployee; j++) {
					distance = calculateDistance(i, j);
					String value = employeeList.get(i) + " and " + employeeList.get(j) + " " + distance;
					context.write(null, new Text(value));
				}
			}
		}

		private int calculateDistance(int i, int j) {
			String employeeA = employeeList.get(i);
			String employeeB = employeeList.get(j);
			int distance = 0;
			
			if(employeeToManagerMap.get(employeeA).equals(employeeB) || employeeToManagerMap.get(employeeB).equals(employeeA)) {
				distance = 0;
			} else if(employeeToManagerMap.get(employeeA).equals(employeeToManagerMap.get(employeeB)) ) {
				distance = 0;
			}else {
				List<String> employeeListA = new ArrayList<String>();
				List<String> employeeListB = new ArrayList<String>();
				employeeListA.add(employeeA);
				String current = employeeA;
				System.out.println("init_current=="+current);
				System.out.println("Test=="+employeeToManagerMap.get(current));
				while(false == employeeToManagerMap.get(current).isEmpty() ) {
					current = employeeToManagerMap.get(current);
					System.out.println("current=="+current);
					employeeListA.add(current);
				}
				
				employeeListB.add(employeeB);
				current = employeeB;
				while(false == employeeToManagerMap.get(employeeB).isEmpty()) {
					current = employeeToManagerMap.get(current);
					employeeListB.add(current);
				}
				
				int ii = 0, jj =0;
				String currentA, currentB;
				boolean found = false;
				for(ii = 0; ii<employeeListA.size(); ii++) {
					currentA = employeeList.get(ii);
					for(jj = 0; jj<employeeListB.size(); jj++) {
						currentB = employeeListB.get(jj);
						if (currentA.equals(currentB)) {
							found = true;
							break;
						}
					}
					
					if (found) {
						break;
					}
				}
				
				distance = ii+jj -1;
			}
			return distance;
		}
	}

}
