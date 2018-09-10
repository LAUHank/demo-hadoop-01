package cn.lhl.mr_vs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
/**
 * 自定义组合键 - 用于二次排序
 * @author Administrator
 *
 */
public class MyKeyWritable implements WritableComparable<MyKeyWritable> {
	
	private String key1;
	private Integer key2;
	
	public MyKeyWritable() {
		set("", 0);
	}
	
	public MyKeyWritable(String key1, Integer key2) {
		set(key1, key2);
	}

	public void set(String key1, Integer key2) {
		this.key1 = key1;
		this.key2 = key2;
	}

	public String getKey1() {
		return key1;
	}

	public void setKey1(String key1) {
		this.key1 = key1;
	}

	public Integer getKey2() {
		return key2;
	}

	public void setKey2(Integer key2) {
		this.key2 = key2;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(key1);
		out.writeInt(key2);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		set(in.readUTF(), in.readInt());
	}

	@Override
	public int compareTo(MyKeyWritable o) {
		int compare = key1.compareTo(o.getKey1());
        if (compare != 0) {
            return compare;
        } else {
            return o.getKey2().compareTo(key2);
        }
	}

	@Override
	public String toString() {
		return "MyKeyWritable [key1=" + key1 + ", key2=" + key2 + "]";
	}

}
