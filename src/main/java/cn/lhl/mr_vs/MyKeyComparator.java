package cn.lhl.mr_vs;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
/**
 * 自定义二次排序逻辑 - 使用组合键
 * @author Administrator
 *
 */
public class MyKeyComparator extends WritableComparator {
	
	protected MyKeyComparator() {
		super(MyKeyWritable.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		if (a instanceof MyKeyWritable && b instanceof MyKeyWritable) {
			MyKeyWritable a1 = (MyKeyWritable) a;
			MyKeyWritable b1 = (MyKeyWritable) b;
			int compare = b1.getKey1().compareTo(a1.getKey1());
	        if (compare != 0) {
	            return compare;
	        } else {
	            return b1.getKey2().compareTo(a1.getKey2());
	        }
		} else {
			return super.compare(b, a);
		}
	}

}
