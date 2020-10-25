package stubs;

import java.io.IOException;
import java.util.Arrays;
import java.lang.String;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class VideoCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	
	private String[] currentVideo = new String[16];
	//logging for debugging purposes
	private static final Logger LOGGER = LogManager.getLogger(VideoCountMapper.class.getName());
	@Override
	public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	    
		//the first row/line of the csv file is the header. we return if first key, else enter.
	    if (key.get() == 0){
	    	return;
	    }
	    else{
	    	LOGGER.info("This is a mapper");
	    	//convert the line to string before operating on
	    	String line = value.toString();
	    	
	    	//this regex splits at every comma followed by a qoute mark. ,"
	    	//directly extracting title and desc. Further operations are comitted to extract likes/trendDate
	    	String[] getColumns = line.split("\\,\"");
	    	
	    	//ensure theres enough strings in the split to operate on. 
	    	//ideally the array should be length of 5, so we can capture all entites based on the regex above
	    	if (getColumns.length == 5){
	    		
	    		/** Use for debugging
	    		 * LOGGER.info("title: " + getColumns[1]);
	    		 * LOGGER.info("desc: " + getColumns[getColumns.length - 1])
	    		 * LOGGER.info("likes: " + getColumns[3].split("\\,(?=[0-9])")[2]);
	    		 */
	    		
	    		//description position tends to be at the end. 
	    		//convert to lowercase (easier to compare with keyword search 'christmas')
	    		//both title and desc have an unnecessary closing quote mark due to regex split. this is replaced
	    		//String desc = getColumns[getColumns.length - 1].replace("\"", "").toLowerCase();
	    		
	    		String title = getColumns[1].replace("\"", "");
	    		
	    	
	    		context.write(new Text(title), new IntWritable(1));
	    		
	    	} 	

	   }     
	}     
}
