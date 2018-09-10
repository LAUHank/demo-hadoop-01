package cn.lhl.mr.xiti.t015;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyLeftJoinReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		
		List<String> alist = new ArrayList<String>();
		List<String> blist = new ArrayList<String>();
		
		for (Text val : values) {
			String valStr = val.toString();
			String[] arr = valStr.split("#");
			if (valStr.startsWith("a#")) {
				alist.add(arr[1]);
			} else if (valStr.startsWith("b#")) {
				blist.add(arr[1]);
			}
			//context.write(key, val);
			/*
			 * c001	b#c001	一班
			 * c001	a#s002	james	c001
			 */
		}
		//left join
		//选择一张小表做左表, 这里选择b
		int bsize = blist.size();
		int asize = alist.size();
		if (bsize > 0) {//left有结果
			for (int i = 0; i < bsize; i++) {//以b为左表
				if (asize > 0) {//a有对应记录
					for (int j = 0; j < asize; j++) {//以a为右表
						context.write(key, new Text(blist.get(i) + "\t" + alist.get(j)));
					}
				} else {//a没有对应记录
					context.write(key, new Text(blist.get(i) + "\t" + "null"+ "\t" + "null"+ "\t" + "null"));
				}
				
			}
		}
	}

}
