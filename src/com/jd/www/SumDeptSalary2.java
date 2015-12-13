/**    
* @Title: SumDeptSalary2.java  
* @Package com.jd.www  
* @Description: TODO(用一句话描述该文件做什么)  
* @author A18ccms A18ccms_gmail_com    
* @date 2015年12月13日 下午8:09:11  
* @version V1.0    
*/

package com.jd.www;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *  
 * 
 * @ClassName: SumDeptSalary2 
 * @Description: TODO(这里用一句话描述这个类的作用) 
 * @author A18ccms a18ccms_gmail_com 
 * @date 2015年12月13日 下午8:09:11      MapReduce中的join分为好几种，比如有最常见的 reduce side
 *       join、map side join和semi join 等。 reduce
 *       join 在shuffle阶段要进行大量的数据传输，会造成大量的网络IO效率低下，而map side
 *       join 在处理多个小表关联大表时非常有用 。 Map side
 *       join是针对以下场景进行的优化：两个待连接表中，有一个表非常大，而另一个表非常小，以至于小表可以直接存放到内存中。
 *       这样我们可以将小表复制多份，让每个map task内存中存在一份（比如存放到hash table中），然后只扫描大表：
 *       对于大表中的每一条记录key/value，在hash table中查找是否有相同的key的记录，如果有，则连接后输出即可。为了支持文件的复制，
 *       Hadoop提供了一个类DistributedCache，使用该类的方法如下：
 *       （1）用户使用静态方法DistributedCache.addCacheFile()指定要复制的文件，它的参数是文件的URI（
 *       如果是HDFS上的文件， 可以这样：hdfs://jobtracker:50030/home/XXX/file）。
 *       JobTracker在作业启动之前会获取这个URI列表， 并将相应的文件拷贝到各个TaskTracker的本地磁盘上。
 *       （2）用户使用DistributedCache.getLocalCacheFiles()方法获取文件目录，
 *       并使用标准的文件读写API读取相应的文件。
 *       在下面代码中，将会把数据量小的表(部门dept ）缓存在内存中，在Mapper阶段对员工部门编号映射成部门名称，
 *       该名称作为key输出到Reduce中，在Reduce中计算按照部门计算各个部门的总工资。
 * 
 * 
 */

public class SumDeptSalary2 {
	public void run(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "SumDeptSalary2");
		job.setJarByClass(com.jd.www.SumDeptSalary2.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);

		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// TODO: specify input and output DIRECTORIES (not files)
		//DistributedCache.addCacheFile(new Path("/in/dept").toUri(), conf);
		FileInputFormat.setInputPaths(job, new Path("/in"));
		FileOutputFormat.setOutputPath(job, new Path("/out"));

		if (!job.waitForCompletion(true))
			return;
	}

	public static void main(String[] args) throws Exception {
		SumDeptSalary2 sds2 = new SumDeptSalary2();
		sds2.run(args);
	}

	public static class MyMapper extends Mapper<Object, Text, Text, Text> {

		private Map<String, String> deptMap = new HashMap<String, String>();

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
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] columns = value.toString().split(",");
			if (columns.length == 8) {
				if (deptMap.containsKey(columns[7])) {
					context.write(new Text(deptMap.get(columns[7].trim())), new Text(columns[5].trim()));
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
		protected void setup(Context context) {
			// TODO Auto-generated method stub
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String fileName = fileSplit.getPath().getName();
			BufferedReader in = null;
			String line = null;
			context.getCounter("USEGROUPMAP", fileName).increment(1);
			try {
				if (fileName.contains("dept")) {
					in = new BufferedReader(new FileReader(fileSplit.getPath().toString()));
					while ((line = in.readLine()) != null) {
						context.getCounter("USEGROUPMAP", line).increment(1);
						deptMap.put(line.split(",")[0], line.split(",")[1]);
					}
				}
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
				try {
					if (null != in) {
						in.close();
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

	}

	public static class MyReducer extends Reducer<Text, Text, Text, IntWritable> {

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
			int sum = 0;
			for(Text val : values) {
				sum += Integer.parseInt(val.toString());
			}
			context.write(key, new IntWritable(sum));
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
		 * org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce
		 * .Reducer.Context) 
		 */

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.setup(context);
		}

	}
}
