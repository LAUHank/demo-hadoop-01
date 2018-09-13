package cn.lhl.mr_vs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
/**
 * 自定义MR驱动类
 * @author Administrator
 *
 */
public class MyMRDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf);
		
		//-Dmapreduce.job.name="jobName"
		//job.setJobName(args[0]);
		
		job.setJarByClass(MyMRDriver.class);
		job.setMapperClass(MyMapper.class);		
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(MyKeyWritable.class);
		job.setOutputValueClass(NullWritable.class);
		
		//-Dmapred.output.key.comparator.class=cn.lhl.mr_vs.MyKeyComparator
		//job.setSortComparatorClass(MyKeyComparator.class);//二次排序
		job.setPartitionerClass(MyPartitioner.class);//分区
		job.setGroupingComparatorClass(MyWritableComparator.class);//分组
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) {
		/*
yarn jar a.jar cn.lhl.mr_vs.MyMRDriver \
-Dmapred.output.key.comparator.class=cn.lhl.mr_vs.MyKeyComparator \
-Dmapreduce.job.queuename=oncourse \
-Dmapred.output.compress=true \
-Dmapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec \
-Dmapreduce.job.name="mr-auxsort" \
/user/lhl13/valsort \
/user/lhl13/xiti_out/sort_gz_21
		 */
		int status = -1;
		try {
			status = ToolRunner.run(new MyMRDriver(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(status);
	}

}
