package stubs;

import org.apache.hadoop.fs	.Path;		
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;


public class VideoCount {
	
	  
	  public static void main(String[] args) throws Exception {

	    if (args.length != 2) {
	      System.out.printf("Usage: ChristmasVideo <input dir> <output dir>\n");
	      System.exit(-1);
	    }
	    
	    //create new job and assign jar class and job name 
	    Job job = new Job();
	    job.setJarByClass(VideoCount.class);
	    job.setJobName("Video Count");
	    
	    //input loc is the first arg, output loc is second
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    //output relevant to <title, likes>, <Text, IntWritable> 
	    job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		
	   
	    job.setMapperClass(VideoCountMapper.class);
	    job.setReducerClass(VideoCountReducer.class);
		job.setCombinerClass(VideoCountReducer.class);
		
	    //check if combiner (reducer) exists
	    if (job.getCombinerClass() == null) {
	      throw new Exception("Combiner not set");
	    }

	    boolean success = job.waitForCompletion(true);
	    System.exit(success ? 0 : 1);
	  }
	 
	}
