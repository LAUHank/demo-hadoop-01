package cn.lhl.hdfs;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import org.junit.Test;

public class HdfsTest {
	
	@Test
	public void test1() {
		System.out.println("hello test1");
	}
	
	@Test
	public void test2() {
		String str = "a b a c d d a";
		Map<String, Integer> map = new HashMap<String, Integer>();
		StringTokenizer st = new StringTokenizer(str);
		while (st.hasMoreTokens()) {
			String s = st.nextToken();
			if (map.containsKey(s)) {
				Integer v = map.get(s);
				v += 1;
				map.put(s, v);
			} else {
				map.put(s, 1);
			}
		}
		List<Map.Entry<String, Integer>> list = new ArrayList<Map.Entry<String, Integer>>();
		for (Map.Entry<String, Integer> me : map.entrySet()) {
			list.add(me);
		}
		Collections.sort(list, new Comparator<Map.Entry<String, Integer>> () {

			@Override
			public int compare(Entry<String, Integer> o1,
					Entry<String, Integer> o2) {
				return o2.getValue() - o1.getValue();
			}
			
		});
		System.out.println(list);
	}
}
