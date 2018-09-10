package cn.lhl.mr_vs;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * 自定义mapper
 * @author Administrator
 *
 */
public class MyMapper extends Mapper<Object, Text, MyKeyWritable, NullWritable> {

	private MyKeyWritable keyOut = new MyKeyWritable();
	private static final NullWritable VAL_OUT = NullWritable.get();

	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] arr = line.split("=");
		keyOut.set(arr[0], Integer.parseInt(arr[1]));
		context.write(keyOut, VAL_OUT);
	}

}
