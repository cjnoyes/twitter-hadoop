package com.cjnoyessw.twitter.hadoop.mapper;

import java.io.IOException;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.io.*;

public class RetweetScreennameCountMapper implements Mapper<LongWritable, Text, Text, IntWritable> {

	public RetweetScreennameCountMapper() {
	}

	
	@Override
	public void configure(JobConf arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void map(LongWritable k, Text value, OutputCollector<Text, IntWritable> collector,
			Reporter reporter) throws IOException {
		String str = value.toString();
		str = str.replaceAll("\\?","");
		String [] parts = str.split("\t");
		if (parts.length < 8) {
			return;
		}
		String count = parts[4];
		if (!count.matches("[0-9]+")) {
			return;
		}
		int cnt = 0;
		try {
			cnt = Integer.parseInt(count);
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			return;
		}
		if (cnt == 0) {
			return;
		}
		Text key = new Text();
		key.set(parts[7]);
		collector.collect(key, new IntWritable(cnt));
	}

}
