package com.cjnoyessw.twitter.hadoop.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class TweetRateMapper implements Mapper<LongWritable, Text, Text, IntWritable> {

	private IntWritable one;
	
	public TweetRateMapper() {
		one = new IntWritable(1);
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
			Reporter reporter)
			throws IOException {
		// TODO Auto-generated method stub
		String str = value.toString();
		str = str.replaceAll("\\?","");
		String [] parts = str.split("\t");
		String keyval = parts[0];
		Text key = new Text();
		key.set(keyval);
		collector.collect(key, one);
	}

}
