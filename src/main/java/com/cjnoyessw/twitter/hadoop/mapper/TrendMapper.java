package com.cjnoyessw.twitter.hadoop.mapper;

import java.io.IOException;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.io.*;

public class TrendMapper implements Mapper<LongWritable, Text, Text, IntWritable> {

	public TrendMapper() {
	   one = new IntWritable(1);
	}

	private IntWritable one;
	
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
		if (parts.length < 2) {
			return;
		}
		Text key = new Text();
		key.set(parts[1]);
		collector.collect(key, one);
	}

}
