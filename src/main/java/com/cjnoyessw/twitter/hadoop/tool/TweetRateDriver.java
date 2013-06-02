package com.cjnoyessw.twitter.hadoop.tool;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.cjnoyessw.twitter.hadoop.mapper.TweetRateMapper;
import com.cjnoyessw.twitter.hadoop.reducer.TweetRateReducer;

public class TweetRateDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if ( args.length != 2) {
			System.err.printf("Usage: %s [generic options] <input> <output>", getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		
		JobConf conf = new JobConf(getConf(),getClass());
		conf.setJobName("TweetRate");
		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setMapperClass(TweetRateMapper.class);
		conf.setReducerClass(TweetRateReducer.class);
		conf.setCombinerClass(TweetRateReducer.class);
		
		JobClient.runJob(conf);
		
        return 0;	
	}

	public static void main(String [] args) throws Exception {
		int exitCode = ToolRunner.run(new TweetRateDriver(), args);
		System.exit(exitCode);
	}
}
