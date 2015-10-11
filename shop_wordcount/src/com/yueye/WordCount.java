package com.yueye;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCount {  

	public static class WordCountMap extends  
	Mapper<LongWritable, Text, Text, Text> {  


		private Text mapValue = new Text();  
		private Text mapKey = new Text();  

		public void map(LongWritable key, Text value, Context context)  
				throws IOException, InterruptedException {  

			String fileName = ((FileSplit)context.getInputSplit()).getPath().toString();
			String line = value.toString();  
			StringTokenizer token = new StringTokenizer(line);  
			if(fileName.contains("Price")){
				while (token.hasMoreTokens()) {  
					String item1 = token.nextToken();
					String item2 = token.nextToken();
					mapKey.set(String.valueOf(Integer.parseInt(item1	) + Integer.parseInt(item2)/1000.0));
					System.out.println(mapKey + "+++");
					mapValue.set(item2);
					context.write(mapKey, mapValue);  
					System.out.println("mapKey = " + mapKey + "mapValue = " + mapValue);
				}  
			}
			if(fileName.contains("Name")){
				while (token.hasMoreTokens()) {  
					mapValue.set("name" + token.nextToken());
					mapKey.set(String.valueOf(Integer.parseInt(token.nextToken()) - 0.1));
					context.write(mapKey, mapValue);  
					System.out.println("mapKey = " + mapKey + "mapValue = " + mapValue);
				}  
			}


		}  
	}  

	
	public static class WordCountReduce extends  
	Reducer<Text, Text, Text, Text> {  
		Text itemName = null;
		public void reduce(Text key, Iterable<Text> values,  
				Context context) throws IOException, InterruptedException { 
			int k = 1;
			
			List<Text> queue = new ArrayList<Text>();

			for(Text val : values){
				System.out.println(k++ + " times " + "val =" +  val);
				if(val.toString().startsWith("name")){
					String realName = val.toString().substring(4	);
					itemName = new Text(realName);
					continue;
				} 
				context.write(itemName,val);
			}
		}
	}

	public static void main(String[] args) throws Exception {  
		Configuration conf = new Configuration();  
		Job job = new Job(conf);  
		job.setJarByClass(WordCount.class);  
		job.setJobName("wordcount");  

		job.setMapOutputKeyClass(Text.class);  
		job.setMapOutputValueClass(Text.class);  
		job.setOutputKeyClass(Text.class);  
		job.setOutputValueClass(Text.class);  

		job.setMapperClass(WordCountMap.class);  
		job.setReducerClass(WordCountReduce.class);  

		job.setInputFormatClass(TextInputFormat.class);  
		job.setOutputFormatClass(TextOutputFormat.class);  

		FileInputFormat.addInputPath(job, new Path(args[0]));  
		FileOutputFormat.setOutputPath(job, new Path(args[1]));  

		Path outputPath = new Path(args[1]);
		FileSystem fs = outputPath.getFileSystem(conf);
		fs.delete(outputPath, true);

		job.waitForCompletion(true);  
	}  
}
