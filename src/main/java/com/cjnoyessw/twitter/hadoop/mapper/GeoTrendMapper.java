/**
 * 
 */
package com.cjnoyessw.twitter.hadoop.mapper;

import java.io.IOException;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.io.*;

/**
 * @author Christopher J. Noyes
 *
 */
public class GeoTrendMapper implements Mapper<LongWritable, Text, Text, IntWritable> {

	public GeoTrendMapper() {
		one = new IntWritable(1);
	}
	
	
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void configure(JobConf arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void map(LongWritable k, Text val, OutputCollector<Text, IntWritable> collector, Reporter reporter) throws IOException {
		// TODO Auto-generated method stub
		
		String str = val.toString();
		str = str.replaceAll("\\?","");
		String [] parts = str.split("\t");
		if (parts.length < 9) {
			return;
		}
		String code = parts[1];
		String country = parts[2];
		String subtype = parts[5];
		String loc = parts[3];
		String trend = parts[8];
		Text key = new Text();
		key.set(code + ":" + country + ":" + subtype + ":" + loc + ":" + trend);
		collector.collect(key, one);
	}

	private IntWritable one;
	
}
