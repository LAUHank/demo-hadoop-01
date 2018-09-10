package cn.lhl.mr;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<Object, Text, Text, IntWritable> {

	private Text keyOut = new Text();
	private static final IntWritable VAL_OUT = new IntWritable(1);

	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
		StringTokenizer itr = new StringTokenizer(value.toString());
		
		while (itr.hasMoreTokens()) {
			String element = itr.nextToken();
			keyOut.set(element);
			context.write(keyOut, VAL_OUT);
		}
		
	}

}
