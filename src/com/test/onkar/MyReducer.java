package com.test.onkar;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * Reducer logic for 
 * Calculating avg of values based on window size
 * */

public class MyReducer{
	
	//Write final output as Window Number->Avg of values in that window
	public static class StockReducer extends Reducer<IntWritable,Text,IntWritable,DoubleWritable>{
		
		public void reduce(IntWritable windowNumber,Iterable<Text> values,Context context) throws IOException, InterruptedException{
			
			//To Hold values in current window number
			List<Integer> allStockValues = new ArrayList<Integer>();
			int counter=0; 
			
			//Get Window size from Job configuration to calculate Avg of values.
			Configuration conf = context.getConfiguration();
			int windowSize = Integer.valueOf(conf.get("WindowSize"));
			//Iterate over values from same key
			//We will get list of Text in case element corresponding to some window is coming from different partitions(mappers)
			
			for(Text value:values){
				//Split values from mapper by ',' and the by ':' to get value
				String data[] = value.toString().split(",");
				
				for(int i=0;i<data.length;i++){
					String val = data[i];
					//Store all values for a window
					allStockValues.add(Integer.valueOf(val.split(":")[1]));
					counter++;
				}
				
			}
			
			//Lets calculate Avg of values if they form window of given size.
			int stockSum=0;
			double avgValue=0;
			for(int i=0;i<allStockValues.size();i++){
				stockSum=stockSum+allStockValues.get(i);
			}
			
			avgValue = ((double)stockSum)/windowSize;
			
			//Write to context as WindowNumber->Avg of Window only if it forms proper window
			if(allStockValues.size()==windowSize){
				context.write(windowNumber, new DoubleWritable(avgValue));	
			}
			
		}//End of reducer
		
		
	}


}
