package com.cjnoyessw.twitter.hadoop.reducer;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapred.Reducer;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.io.*;

public class WordFrequencyReducer implements Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	public void configure(JobConf arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void reduce(Text key, Iterator<IntWritable> values,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		// TODO Auto-generated method stub
		int maxValue = 0;
		while (values.hasNext()) {
			maxValue += values.next().get();
		}
		output.collect(key, new IntWritable(maxValue));
	}

}
