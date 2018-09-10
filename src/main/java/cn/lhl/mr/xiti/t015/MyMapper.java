package cn.lhl.mr.xiti.t015;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class MyMapper extends Mapper<Object, Text, Text, Text> {

	private Text keyOut = new Text();
	private Text valOut = new Text();
	private String pathName = "";
	
	@Override
	protected void setup(Mapper<Object, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		
		InputSplit is = context.getInputSplit();
		if (is instanceof FileSplit) {
			FileSplit fis = (FileSplit) is;
			Path p = fis.getPath();
			pathName = p.getName();
		}
	}



	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		String[] arr = line.split("\\t");
		
		if (pathName.contains("student")) {//表名
			keyOut.set(arr[2]);
			valOut.set("a#"+value);//别名
			context.write(keyOut, valOut);
		} else if (pathName.contains("class_info")) {//表名
			keyOut.set(arr[0]);
			valOut.set("b#"+value);//别名
			context.write(keyOut, valOut);
		}
	}

}
