package com.hadoop.WordCount;

import java.io.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	public void map( LongWritable key, Text value, Context context ) 
		throws IOException, InterruptedException{
		
		String arrWords[] = value.toString().split( " " );
		for( String word : arrWords ) {
			context.write( new Text(word), new IntWritable( 1 ) );
		}
	}
}
