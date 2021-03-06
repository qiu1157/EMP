/**    
* @Title: SalaryTop3.java  
* @Package com.jd.www  
* @Description: TODO(用一句话描述该文件做什么)  
* @author A18ccms A18ccms_gmail_com    
* @date 2015年12月23日 下午10:26:34  
* @version V1.0    
*/

package com.jd.www;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**  
* @ClassName: SalaryTop3  
* @Description: TODO(这里用一句话描述这个类的作用)  
* @author   
* @date 2015年12月23日 下午10:26:34  
*    
*/

public class SalaryTop3 {
	public void run(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "SalaryTop3");
		job.setJarByClass(com.jd.www.SalaryTop3.class);
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
		SalaryTop3 st = new SalaryTop3();
		st.run(args);
	}

	public static class MyMapper extends Mapper<Object, Text, Text, Text> {

		/* (非 Javadoc)  
		* <p>Title: map</p>  
		* <p>Description: </p>  
		* @param key
		* @param value
		* @param context
		* @throws IOException
		* @throws InterruptedException  
		* @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)  
		*/
		
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] columns = value.toString().split(",");
			if (null != columns || columns.length > 1) {
				context.write(new Text("a"), new Text(columns[1]+"+"+columns[5]));
			}
		}
		
	}
	
	public static class MyReducer extends Reducer<Text, Text, Text, Text> {

		/* (非 Javadoc)  
		* <p>Title: reduce</p>  
		* <p>Description: </p>  
		* @param arg0
		* @param arg1
		* @param arg2
		* @throws IOException
		* @throws InterruptedException  
		* @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)  
		*/
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Map<String, Long> emp = new TreeMap<String, Long>();
			for (Text value : values) {
				emp.put(value.toString().split("\\+")[0], Long.parseLong(value.toString().split("\\+")[1]));
			}
			List<Map.Entry<String, Long>> list = new ArrayList<Map.Entry<String, Long>>(emp.entrySet());
			Collections.sort(list, new Comparator<Map.Entry<String, Long>>() {

				@Override
				public int compare(Entry<String, Long> o1, Entry<String, Long> o2) {
					// TODO Auto-generated method stub
					return o2.getValue().compareTo(o1.getValue());
				}
			});
			
			int counter = 0;
			for (Map.Entry<String, Long> mapping : list) {
				counter++;
				if(counter > 3) {
					break;
				}
				context.write(new Text(mapping.getKey()), new Text(mapping.getValue()+""));
			}
		}
	}
}
