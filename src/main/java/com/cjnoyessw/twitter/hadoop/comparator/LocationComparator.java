package com.cjnoyessw.twitter.hadoop.comparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.Text;

public class LocationComparator extends WritableComparator {

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		// TODO Auto-generated method stub
		Text t1 = (Text)a;
		Text t2 = (Text)b;
		String s1 = t1.toString();
		String s2 = t2.toString();
		String [] p1 = s1.split(":");
		String [] p2 = s1.split(":");
	
		for (int i =0; i < p1.length; i++) {
			int v = p1[i].compareTo(p2[i]);
			if (v != 0) {
				return v;
			}
		}
		return 0;
	}

	protected LocationComparator() {
		super(Text.class,true);
		// TODO Auto-generated constructor stub
	}

}
