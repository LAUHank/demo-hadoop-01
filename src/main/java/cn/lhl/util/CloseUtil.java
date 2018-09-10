package cn.lhl.util;

public class CloseUtil {
	public static void close(AutoCloseable closeObj) {
		if (closeObj != null) {
			try {
				closeObj.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
