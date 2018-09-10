package cn.lhl.mr.xiti.t015;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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
		Job job = Job.getInstance(conf, args[0]);
		
		job.setJarByClass(MyMRDriver.class);
		
		job.setMapperClass(MyMapper.class);		
		//job.setReducerClass(MyReducer.class);
		job.setReducerClass(MyLeftJoinReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) {
		/*
yarn jar a.jar cn.lhl.mr.xiti.t015.MyMRDriver \
-Dmapreduce.job.queuename=oncourse \
lhl13-mr-xiti-T015-join-01  \
/user/lhl13/join \
/user/lhl13/xiti_out/join_01
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
