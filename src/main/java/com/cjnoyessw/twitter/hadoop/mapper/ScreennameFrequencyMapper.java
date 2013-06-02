package com.cjnoyessw.twitter.hadoop.mapper;

import java.io.IOException;

import java.util.StringTokenizer;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.io.*;
 


public class ScreennameFrequencyMapper implements Mapper<LongWritable, Text, Text, IntWritable> {

	public ScreennameFrequencyMapper() {
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
		int pos = str.indexOf(" @");
		if (pos == -1) {
			return;
		}
		String screenname = null;
		while (pos != -1) {
			pos++;
			int end = str.indexOf(" ", pos);
			if (end == -1) {
				screenname = str.substring(pos);
				if (screenname.endsWith(":")) {
					screenname = screenname.replace(":","");
				}
				screenname = screenname.replaceAll("[!,.]", "");
				Text txt = new Text();
				txt.set(screenname);
				collector.collect(txt, one);
			}
			else {
				screenname = str.substring(pos,end);
				if (screenname.endsWith(":")) {
					screenname = screenname.replace(":","");
				}
				screenname = screenname.replaceAll("[!,.]", "");
				Text txt = new Text();
				txt.set(screenname);
				collector.collect(txt, one);
			}
			pos = str.indexOf(" @", pos);
		}
		
	}

	
	private int col;
}
