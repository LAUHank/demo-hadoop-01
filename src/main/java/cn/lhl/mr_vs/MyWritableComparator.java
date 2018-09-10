package cn.lhl.mr_vs;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
/**
 * 自定义分组逻辑 - 使用原来的自然键
 * @author Administrator
 *
 */
public class MyWritableComparator extends WritableComparator {
	
	protected MyWritableComparator() {
		super(MyKeyWritable.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		if (a instanceof MyKeyWritable && b instanceof MyKeyWritable) {
			MyKeyWritable a1 = (MyKeyWritable) a;
			MyKeyWritable b1 = (MyKeyWritable) b;
			return b1.getKey1().compareTo(a1.getKey1());//a1.compareTo(b1);//
		} else {
			return super.compare(b, a);
		}
	}

}
