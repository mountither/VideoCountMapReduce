package stubs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class VideoCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	private IntWritable videoCount = new IntWritable();
  @Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		
	  	int titleCount = 0;
		
	  	//loop through each value incoming from mapper, count and write them
		for (IntWritable value : values) {
			titleCount += value.get();
		}
		
		videoCount.set(titleCount);
		
		context.write(key, new IntWritable(titleCount));
	}
}