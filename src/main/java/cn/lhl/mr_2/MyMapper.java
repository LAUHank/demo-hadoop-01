package cn.lhl.mr_2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<Object, Text, Text, IntWritable> {

	private Text keyOut = new Text();
	private IntWritable valOut = new IntWritable(1);

	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] arr = line.split("=");
		keyOut.set(arr[0]);
		valOut.set(Integer.parseInt(arr[1]));
		context.write(keyOut, valOut);
	}

}
