package cn.lhl.hdfs;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.common.IOUtils;

import cn.lhl.util.CloseUtil;


public class HdfsIoUtil {
	
	private static Configuration conf = new Configuration();
	private static final String ENC = "utf-8";
	private static final int BUF_SIZE = 65536;
	
	public static void main(String[] args) {
		/*
		 * rz -y a.jar
		 * yarn jar a.jar cn.lhl.hdfs.HdfsIoUtil /user/lhl13/readme
		 */
		String path = "";
		int i = 1;
		for (String arg : args) {
			if (i == 1) {
				path = arg;
			}
			i++;
		}
		String enc = ENC;
		int bufSize = BUF_SIZE;
		try {
			System.out.println(readString(path, enc));
			System.out.println(new String(readBytes(path, bufSize), enc));
			printFile(path);
			System.out.println("done");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static String readString(Path path, String enc, String lineDelim) {
		StringBuffer sb = new StringBuffer();
		FileSystem fs = null;
		BufferedReader br = null;
		try {
			fs = FileSystem.get(conf);
			br = new BufferedReader(new InputStreamReader(fs.open(path), enc));
			String line = null;
			while ((line = br.readLine()) != null) {
				sb.append(line).append(lineDelim);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			CloseUtil.close(br);
			CloseUtil.close(fs);
		}
		return sb.toString();
	}
	
	public static String readString(String path, String enc) {
		return readString(new Path(path), enc, System.lineSeparator());
	}
	
	public static byte[] readBytes(String path, int bufSize) {
		byte[] result = new byte[0];
		FileSystem fs = null;
		FSDataInputStream is = null;
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try {
			fs = FileSystem.get(conf);
			is = fs.open(new Path(path));
			byte[] b = new byte[bufSize];
			int len = -1;
			while ((len = is.read(b)) > 0) {
				baos.write(b, 0, len);
			}
			result = baos.toByteArray();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			CloseUtil.close(baos);
			CloseUtil.close(is);
			CloseUtil.close(fs);
		}
		return result;
	}
	
	public static void printFile(String path) {
		FileSystem fs = null;
		FSDataInputStream is = null;
		try {
			fs = FileSystem.get(conf);
			is = fs.open(new Path(path));
			IOUtils.copyBytes(is, System.out, 4096, false);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(is);
			CloseUtil.close(fs);
		}
	}
	
	public static void walkTree(Path path, StringBuffer sb) {
		FileSystem fs = null;
		try {
			fs = FileSystem.get(conf);
			
			FileStatus fileStatus = fs.getFileStatus(path);
			if (fileStatus.isDirectory()) {
				FileStatus[] fsArr = fs.listStatus(path);
				for (FileStatus fileStatusE : fsArr) {
					walkTree(fileStatusE.getPath(), sb);
				}
			} else if (fileStatus.isFile()) {
				//System.out.println(path);
				String txt = readString(path, "utf-8", " ");
				sb.append(txt);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			CloseUtil.close(fs);
		}
	}
}
