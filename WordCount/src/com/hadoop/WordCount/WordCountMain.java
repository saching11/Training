package com.hadoop.WordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountMain {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration config = new Configuration();
		Job job = new Job( config, "Word Count" );
		
		job.setJarByClass( WordCountMain.class );
		job.setMapperClass( WordCountMapper.class );
		job.setReducerClass( WordCountReducer.class );
		
		job.setOutputKeyClass( Text.class );
		job.setOutputValueClass( IntWritable.class );
		
		FileInputFormat.addInputPath( job, new Path( args[0] ) );
		FileOutputFormat.setOutputPath( job, new Path( args[1] ) );
		
		System.exit( job.waitForCompletion( true ) ? 0 : 1 );
	}

}
