package learning.bigdata.main;

import learning.bigdata.mapreduce.HitsByCountryMapper;
import learning.bigdata.mapreduce.HitsByCountryReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HitsByCountry extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		
		if (args.length < 2)
		{
			System.out.println("Usage: HitsByCountry <comma separated input directories> <output dir>");
			System.exit(-1);
		}
		
		int result = ToolRunner.run(new HitsByCountry(), args);
		System.exit(result);
		
	}
	
	@Override
	public int run(String[] args) throws Exception {
		
		try {
			
			Configuration conf = getConf();
			Job job = Job.getInstance(conf);
			job.setJobName("Calculating hits by country");
			job.setJarByClass(HitsByCountry.class);
			
			// Declare mapper and reducer implementation
			job.setMapperClass(HitsByCountryMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setReducerClass(HitsByCountryReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
			// Set input and output formats for the job
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			
			// Set input and output paths
			FileInputFormat.setInputPaths(job, args[0]);
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			
			// Variable to control job completion
			boolean success = job.waitForCompletion(true);
			
			return success ? 0 : 1;
		} catch (Exception e) {
			e.printStackTrace();
			return 1;
		}
	}
}
