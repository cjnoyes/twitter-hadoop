package com.cjnoyessw.twitter.hadoop.tool;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.io.*;
import com.cjnoyessw.twitter.hadoop.reducer.CountryTrendReducer;
import com.cjnoyessw.twitter.hadoop.mapper.CountryTrendMapper;
import com.cjnoyessw.twitter.hadoop.partitioner.*;
import com.cjnoyessw.twitter.hadoop.comparator.LocationComparator;


public class CountryTrendDriver extends Configured implements Tool {

	@SuppressWarnings("unchecked")
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if ( args.length != 2) {
			System.err.printf("Usage: %s [generic options] <input> <output>", getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		
		JobConf conf = new JobConf(getConf(),getClass());
		conf.setJobName("GeoTrend");
		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		conf.setJarByClass(GeoTrendDriver.class);
		conf.setPartitionerClass((Class<? extends Partitioner>) LocationPartitioner.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setMapperClass(CountryTrendMapper.class);
		conf.setReducerClass(CountryTrendReducer.class);
		conf.setCombinerClass(CountryTrendReducer.class);
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(IntWritable.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setOutputKeyComparatorClass(LocationComparator.class);
		conf.setOutputValueGroupingComparator(LocationComparator.class);
		
		JobClient.runJob(conf);
		
        return 0;	
	}

	
	public static void main(String [] args) throws Exception {
		int exitCode = ToolRunner.run(new TrendDriver(), args);
		System.exit(exitCode);
	}
	
	
}
