package cn.lhl.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MyMRDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		/*
		for (Entry<String, String> entry : conf) {
			String k = entry.getKey();
			String v = entry.getValue();
			System.out.printf("%s=%s"+System.lineSeparator(), k, v);
		}
		*/
		Job job = Job.getInstance(conf, args[0]);
		
		job.setJarByClass(MyMRDriver.class);
		
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
yarn jar a.jar cn.lhl.mr.MyMRDriver \
-Dmapreduce.job.queuename=oncourse \
lhl-wc-01 \
/user/lhl13/xiti/t004 \
/user/lhl13/xiti_out/t004_3
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
