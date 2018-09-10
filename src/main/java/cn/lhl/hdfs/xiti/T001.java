package cn.lhl.hdfs.xiti;

import java.io.FileInputStream;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.common.IOUtils;

import cn.lhl.util.CloseUtil;

public class T001 {

	/*
	 * 1、下载www.baidu.com首页的文本数据，写入到本地文件index.html中，并将文件通过java api的方式上传到/tmp/自己的登陆用户名的目录中。

linux wget下载www.baidu.com首页的文本内容，命名为index.html 
通过java代码读取本地的index.html文件成java字节数组。 
通过java代码打开hdfs要写入文件的输出流。 
通过java代码已经打开的输出流，写入之前读取完成的java字节数组。写入完成，将输出流关闭。
通过java代码写主方法，传入本地的输入文件和hdfs的输出文件路径。 
maven打包上传到开发环境，用yarn jar来执行。 
执行完成后查看传入java代码的hdfs的输出文件路径是否已存在，内容是否正确。
	 */
	private static Configuration conf = new Configuration();
	
	public static void main(String[] args) {
		/*
		 * wget www.baidu.com
		 * rz -y a.jar
		 * yarn jar a.jar cn.lhl.hdfs.xiti.T001 /home/hdfs/lhl13/index.html /user/lhl13/index.html
		 */
		String fromPath = args[0];
		String toPath = args[1];
		copy(fromPath, toPath);
		System.out.println("复制完成");
	}
	
	public static void copy(String fromPath, String toPath) {
		InputStream is = null;
		FileSystem fs = null;
		FSDataOutputStream os = null;
		try {
			is = new FileInputStream(fromPath);
			fs = FileSystem.get(conf);
			os = fs.create(new Path(toPath));
			IOUtils.copyBytes(is, os, 4096, false);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			CloseUtil.close(os);
			CloseUtil.close(fs);
			CloseUtil.close(is);
		}
	}

}
