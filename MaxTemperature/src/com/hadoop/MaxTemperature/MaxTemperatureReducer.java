package com.hadoop.MaxTemperature;

import java.io.*;

// Hadoop Reducer packages
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxTemperatureReducer extends Reducer< Text, IntWritable, Text, IntWritable > {

	public void reduce( Text key, Iterable<IntWritable> values, Context context )
		throws IOException, InterruptedException {
		
		int maxValue = Integer.MIN_VALUE;
		for( IntWritable value : values ) {
			maxValue = Math.max( value.get(), maxValue );
		}
		
		context.write( key, new IntWritable( maxValue ) );
		
	}
}
