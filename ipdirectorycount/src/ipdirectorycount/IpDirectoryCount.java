package ipdirectorycount;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class IpDirectoryCount {
	
	public static class myMapper extends Mapper<Object, Text, Text, IntWritable>{
		
		private final static IntWritable one = new IntWritable(1);
		private Text ipdirectory= new Text();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] strs = value.toString().split(" ");
			ipdirectory.set(strs[0] + " " + strs[6]);
			context.write(ipdirectory, one);
	
		}
	}
	
	public static class myReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		private IntWritable result = new IntWritable();
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			
			int sum = 0;
			for(IntWritable val : values){
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
		
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if(otherArgs.length != 2){
			System.err.println("There is a error argument of path. Usage: class <input path> <output path>");
			System.exit(2);
		}
		Job job = new Job(conf, "yueye's ip and directory count");
		job.setJarByClass(IpDirectoryCount.class);
		job.setMapperClass(myMapper.class);
		job.setCombinerClass(myReducer.class);
		job.setReducerClass(myReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
