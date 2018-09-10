package cn.lhl.mr.xiti;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import cn.lhl.util.CloseUtil;
/**
 * side_data - Configuration实现
 * @author Administrator
 *
 */
public class T005 extends Configured implements Tool {
	/*
在hdfs目录/tmp/tianliangedu/input/wordcount目录中有一系列文件，内容为","号分隔，
同时在hdfs路径/tmp/tianliangedu/black.txt黑名单文件，一行一个单词用于存放不记入统计的单词列表。
求按","号分隔的各个元素去除掉黑名单后的出现频率，输出到目录/tmp/tianliangedu/output/个人用户名的hdfs目录中。
	 */
	private static class MyMapper extends Mapper<Object, Text, Text, IntWritable> {
		
		private Text keyOut = new Text();
		private static final IntWritable VAL_OUT = new IntWritable(1);
		private static Set<String> blackList;
		
		
		// setup 只会在每个map任务开始时调用一次
		// 一个分片对应一个map任务
		// 一个分片包含多条记录
		@Override
		protected void setup(
				Mapper<Object, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			super.setup(context);
			
			Configuration conf = context.getConfiguration();
			String fileName = conf.get("black_list_file");
			Path path = new Path(fileName); // 黑名单文件 一行一个单词
			String enc = "utf-8";
			
			blackList = new HashSet<String>();
			
			FileSystem fs = null;
			BufferedReader br = null;
			try {
				fs = FileSystem.get(conf);
				br = new BufferedReader(new InputStreamReader(fs.open(path), enc));
				String line = null;
				while ((line = br.readLine()) != null) {
					blackList.add(line);
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				CloseUtil.close(br);
				//CloseUtil.close(fs);
			}
			System.out.println(blackList);
		}


		// map 每条记录都会调用一次
		@Override
		protected void map(Object key, Text value,
				Mapper<Object, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			
			while (itr.hasMoreTokens()) {
				String element = itr.nextToken();
				if (blackList != null && blackList.contains(element)) {
					// do nothing
				} else {
					keyOut.set(element);
					context.write(keyOut, VAL_OUT);
				}
				
			}
		}
		
	}
	
	private static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		private IntWritable valOut = new IntWritable(1);
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			valOut.set(sum);
			context.write(key, valOut);
		}
		
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		
		conf.set("black_list_file", args[3]);
		
		Job job = Job.getInstance(conf, args[0]);
		
		job.setJarByClass(T005.class);
		
		job.setMapperClass(MyMapper.class);
		//job.setMapOutputKeyClass(Text.class);
		//job.setMapOutputValueClass(IntWritable.class);
		
		//job.setCombinerClass(MyReducer.class);
		
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) {
		/*
yarn jar a.jar cn.lhl.mr.xiti.T005 \
-Dmapreduce.job.queuename=oncourse \
lhl13-mr-xiti-T005 \
/user/lhl13/wordcount \
/user/lhl13/xiti_out/t005 \
/user/lhl13/xiti/t005/black_list
		 */
		int status = -1;
		try {
			status = ToolRunner.run(new T005(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(status);
	}

}
