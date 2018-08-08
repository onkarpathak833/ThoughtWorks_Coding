package com.test.onkar;

import org.apache.hadoop.mapreduce.Mapper;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * Mapper Code
 * Data preparation phase for Reducer
 * This Mapper takes care of data in a window
 * coming from multiple partitions.
 * */

public class MyMapper {
	
	String data = "0,1#1,2#2,3#3,4#4,5#5,6#6,7#7,8#8,9#9,10";
	
	public static class StockMapper extends Mapper<LongWritable, Text, IntWritable,Text>{
		public  int windowSize = 0;
		@SuppressWarnings("unchecked")
		public void map(LongWritable key, Text text,Context context) throws IOException, InterruptedException{
			int currentWindowIndex = 0;
			String indexToWrite = "";
			String data = text.toString();
			String indexData[] = data.split("#");
			String windowNumbers = "";
			int windows = 0;
			
			// Access window size passed from command line argument.
			Configuration conf =  context.getConfiguration();
			windowSize = Integer.valueOf(conf.get("WindowSize"));
			int tempWindowSize = windowSize;
			Map indexWindowMap = new HashMap<Integer,List<String>>();
			Map<Integer,List<String>> windowIndicesMap = new HashMap<Integer,List<String>>();
			
			//Sample file for writing logs to local filesystem.
			//Used only for debugging purpose
			File file = new File("/home/acadgild/Onkar/MapperLogs.txt");
			if(!file.exists()){
				  file.createNewFile();
			}
			
			FileWriter writer = new FileWriter(file);
			for(int i=0;i<indexData.length;i++){
				//indexToWrite = indexToWrite+currentWindowIndex+","+currentWindowIndex+1+","+currentWindowIndex+2;
				int currentPos = i;
				
				//get current index from data
				int currentIndex = Integer.valueOf(indexData[i].split(",")[0]);
				int tempCurrentIndex = currentIndex;
				
				//Calculate window in which each element will be used.
				/*Handle case for index which will be part of less windows
				 * i.e. index 0 and 1 for window size 3 etc */
				
				//Generate intermediate key value structure to hold in which windows each element will be used
				// HasMap "indexWindowMap" tells in which windows every element will be used 
				if(currentIndex<=windowSize-2){
					windowNumbers="";
					for(int k=currentIndex;k>=0;k--){
						windowNumbers=windowNumbers+","+tempCurrentIndex;
						tempCurrentIndex--;
					}
					if(windowNumbers.startsWith(",")){
						windowNumbers = windowNumbers.replaceFirst(",", "");
					}
					indexWindowMap.put(currentIndex, Arrays.asList(windowNumbers.split(",")));
					//context.write(new IntWritable(currentIndex), new Text(windowNumbers));
					writer.write("index number : "+currentIndex+", windows : "+Arrays.toString(windowNumbers.split(",")));
				}
				else {
					int j=0;
					windowNumbers="";
					while(j<tempWindowSize){
						
							windowNumbers = windowNumbers+","+(tempCurrentIndex+0);
							tempCurrentIndex = tempCurrentIndex-1;
							//currentIndex = tempCurrentIndex;
							j++;
						
					}
					if(windowNumbers.startsWith(",")){
						windowNumbers=windowNumbers.replaceFirst(",", "");
					}
					indexWindowMap.put(currentIndex, Arrays.asList(windowNumbers.split(",")));
					writer.write("index number : "+currentIndex+", windows : "+Arrays.toString(windowNumbers.split(",")));
					//context.write(new IntWritable(currentIndex), new Text(windowNumbers));
					
				}
				
				
			}
			
			//Transform the element index->window numbers mapping to window number->list of elements data structure
			
			Iterator<Map.Entry<Integer,List<String>>> itr = indexWindowMap.entrySet().iterator();
			//Map.Entry entry =  indexWindowMap.entrySet();
			while(itr.hasNext()){
				Map.Entry pair = itr.next();
				String indexNumber = String.valueOf(pair.getKey());
				List<String> windowNums = (List) pair.getValue();
				for(int i=0;i<windowNums.size();i++){
					if(windowIndicesMap.containsKey(Integer.valueOf(windowNums.get(i)))){
						List tempList = windowIndicesMap.get(Integer.valueOf(windowNums.get(i)));
						tempList.add(indexNumber+":"+indexData[Integer.valueOf(indexNumber)].split(",")[1]);
						windowIndicesMap.put(Integer.valueOf(windowNums.get(i)), tempList);
					}
					else{
						List tempList = new ArrayList<String>();
						tempList.add(indexNumber+":"+indexData[Integer.valueOf(indexNumber)].split(",")[1]);
						windowIndicesMap.put(Integer.valueOf(windowNums.get(i)),tempList);
					}
				}
			}
			// Use window->Element index mapping above to write to Map Context
			// mapper output will be window number->List of elements used
			//Even if data from other partition correspond to this window it will written and aggregated in reducer
			Iterator<Map.Entry<Integer,List<String>>> windowIterator = windowIndicesMap.entrySet().iterator();
			while(windowIterator.hasNext()){
				Map.Entry<Integer, List<String>> pair = windowIterator.next(); 
				int windowNumber = pair.getKey();
				List list = pair.getValue();
				writer.write("window number : "+windowNumber+", Index : "+Arrays.toString(list.toArray()));
				String indexAndValues = "";
				for(int i=0;i<list.size();i++){
					indexAndValues=indexAndValues+","+list.get(i);
				}
				if(indexAndValues.startsWith(",")){
					indexAndValues = indexAndValues.replaceFirst(",", "");
				}
				System.out.println("window number : "+windowNumber+", Index : "+Arrays.toString(list.toArray()));
				context.write(new IntWritable(windowNumber), new Text(indexAndValues));
			}
			writer.close();
			
		} // End of Map function
		
	}
	
	
	

}
