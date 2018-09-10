package cn.lhl.mr.xiti.t015;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text, Text, Text, Text> {

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
		//inner join
		int asize = alist.size();
		int bsize = blist.size();
		if (asize > 0 && bsize > 0) {//inner有结果
			for (int i = 0; i < asize; i++) {//以a为左表
				for (int j = 0; j < bsize; j++) {//以b为右表
					context.write(key, new Text(alist.get(i) + "\t" + blist.get(j)));
				}
			}
		}
	}

}
