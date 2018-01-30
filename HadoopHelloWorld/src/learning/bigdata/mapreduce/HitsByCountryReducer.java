package learning.bigdata.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HitsByCountryReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	private Text outputKey = new Text();
	private IntWritable outputValue = new IntWritable();
	private int count = 0;
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		
		// Process every unique key emitted from the Mapper
		count = 0;
		Iterator<IntWritable> iter = values.iterator();
		while (iter.hasNext()) {
			IntWritable value = iter.next();
			count += value.get();
		}
		
		// Now let's set the output
		outputKey.set(key);
		outputValue.set(count);
		context.write(outputKey, outputValue);
		
		
	}
}
