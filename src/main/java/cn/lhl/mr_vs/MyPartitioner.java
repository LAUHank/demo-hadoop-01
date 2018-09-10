package cn.lhl.mr_vs;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;
/**
 * 自定义分区逻辑 - 使用原来的自然键
 * @author Administrator
 *
 */
public class MyPartitioner extends Partitioner<MyKeyWritable, NullWritable> {

	@Override
	public int getPartition(MyKeyWritable key, NullWritable value,
			int numPartitions) {
		return (key.getKey1().hashCode() & Integer.MAX_VALUE) % numPartitions;
	}

}
