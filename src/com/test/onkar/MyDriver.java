package com.test.onkar;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.test.onkar.MyMapper.StockMapper;
import com.test.onkar.MyReducer.StockReducer;
/* author : Onkar
 * Driver class for Mapper and reducers
 * E.g. to Run MR Job
 * hadoop jar /home/acadgild/Onkar/Test2.jar com.test.onkar.MyDriver /TestData.txt /Stockoutput.txt 4
 * */

public class MyDriver {

	
		// TODO Auto-generated method stub
		public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
			// TODO Auto-generated method stub
			
			Configuration conf = new Configuration();
			
			// Pass Window size from commnd line argument and access it via job context
			conf.set("WindowSize", args[2]);
			//conf.set("io.sort.mb", "20");
			Job job = new Job(conf,"Test");
			job.setJarByClass(MyDriver.class);
			job.setMapperClass(StockMapper.class);
			job.setReducerClass(StockReducer.class);
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(Text.class);
			job.setNumReduceTasks(1);
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			//job.setCombinerClass(Combiner1.class);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			
			job.waitForCompletion(true);
		}
	

}
