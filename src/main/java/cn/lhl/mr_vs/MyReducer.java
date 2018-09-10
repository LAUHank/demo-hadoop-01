package cn.lhl.mr_vs;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
/**
 * 自定义reducer
 * @author Administrator
 *
 */
public class MyReducer extends Reducer<MyKeyWritable, NullWritable, MyKeyWritable, NullWritable> {

	@Override
	protected void reduce(MyKeyWritable key, Iterable<NullWritable> values,
			Context context) throws IOException, InterruptedException {
		
		for (NullWritable val : values) {
			context.write(key, val);
			break;
		}
	}

}
