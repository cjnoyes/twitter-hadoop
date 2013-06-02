package com.cjnoyessw.twitter.hadoop.partitioner;

import org.apache.hadoop.mapreduce.Partitioner;

import org.apache.hadoop.io.*;

public class LocationPartitioner extends Partitioner<Text, IntWritable> {

	@Override
	public int getPartition(Text key, IntWritable value, int numPartitions) {
		// TODO Auto-generated method stub
		String [] parts = key.toString().split(":");
		StringBuilder bld = new StringBuilder();
		for (int i = 0; i < parts.length-1; i++) {
			bld.append(parts[i]);
			bld.append(":");
		}
		return bld.toString().hashCode() % numPartitions;
	}

}
