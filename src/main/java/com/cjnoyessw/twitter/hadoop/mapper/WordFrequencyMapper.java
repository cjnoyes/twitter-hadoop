package com.cjnoyessw.twitter.hadoop.mapper;

import java.io.IOException;

import java.util.StringTokenizer;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.io.*;
 


public class WordFrequencyMapper implements Mapper<LongWritable, Text, Text, IntWritable> {

	public WordFrequencyMapper() {
	   one = new IntWritable(1);
	   col = 0;
	}

	private IntWritable one;
	
	@Override
	public void configure(JobConf conf) {
		// TODO Auto-generated method stub
		String val = conf.get("analyze_col");
		if (val.matches("[0-9]+")) {
			col = Integer.parseInt(val);
		}
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
		if (col > parts.length-1) {
			return;
		}
		str = parts[col];
		StringTokenizer tokr = new StringTokenizer(str," ,./?\\-+';:|+={}[]\"&%!<>");
		while (tokr.hasMoreTokens()) {
			String tok = tokr.nextToken().trim();
			if (tok.startsWith("@")) {
				continue;
			}
			tok = tok.substring(0, 1).toUpperCase()+tok.substring(1).toLowerCase();
			if (tok.length() > 3) {
				Text key = new Text();
				key.set(tok);
				collector.collect(key, one);
			}
		}
	}

	
	private int col;
}
