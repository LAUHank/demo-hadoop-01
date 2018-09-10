package cn.lhl.hdfs.xiti;

import org.apache.hadoop.fs.Path;

import cn.lhl.hdfs.HdfsIoUtil;
import cn.lhl.util.MyUtil;

public class T004 {

	/*
	 * 有一个hdfs数据目录/tmp/data/中，存放若干txt文本文件，文件内部存放的是由空格分隔的中英文单词，
	 * 求该目录下所有单词的出现频率，并按出现频率倒序排列后输出到控制台中。
	 */
	public static void main(String[] args) {
		//yarn jar a.jar cn.lhl.hdfs.xiti.T004 /user/lhl13/xiti/t004
		StringBuffer sb = new StringBuffer();
		HdfsIoUtil.walkTree(new Path(args[0]), sb);
		System.out.println(sb);
		MyUtil.printWorldCount(sb.toString());
	}

}
