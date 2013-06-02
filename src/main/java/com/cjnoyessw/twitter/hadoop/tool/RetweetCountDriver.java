package com.cjnoyessw.twitter.hadoop.tool;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.io.*;
import com.cjnoyessw.twitter.hadoop.reducer.RetweetCountReducer;
import com.cjnoyessw.twitter.hadoop.mapper.RetweetCountMapper;


public class RetweetCountDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if ( args.length != 2) {
			System.err.printf("Usage: %s [generic options] <input> <output>", getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		
		JobConf conf = new JobConf(getConf(),getClass());
		conf.setJobName("RetweetCount");
		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setMapperClass(RetweetCountMapper.class);
		conf.setReducerClass(RetweetCountReducer.class);
		conf.setCombinerClass(RetweetCountReducer.class);
		
		JobClient.runJob(conf);
		
        return 0;	
	}

	
	public static void main(String [] args) throws Exception {
		int exitCode = ToolRunner.run(new RetweetCountDriver(), args);
		System.exit(exitCode);
	}
	
	
}
