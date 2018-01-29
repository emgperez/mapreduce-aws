package learning.bigdata.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper to process the input log file
 * It'll find the country to which the IP belongs and will emit a key-value pair with the pattern <country, 1>
 * 
 */


public class HitsByCountryMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	// Instead of using Geoip for example to determine the IP, we'll generate fake countries
	private final static String[] COUNTRIES = {"India", "UK", "US", "China"};
	
	private Text outputKey = new Text();

	private IntWritable outputValue = new IntWritable();

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		// File lines are expected to be in the format "Date, IP"
		try {

			String valueString = value.toString();
			
			// Split the line
			String[] row = valueString.split(",");
			
			// IP is the second field
			String ipAddress = row[1];

			// Generate the country randomly
			String countryName = getCountryNameFromIpAddress(ipAddress);
			outputKey.set(countryName);
			outputValue.set(1); // <country, 1> = key-value pair
			context.write(outputKey, outputValue);
 			
		} catch (ArrayIndexOutOfBoundsException ex) {

			context.getCounter("Custom counters", "MAPPER_EXCEPTION_COUNTER").increment(1);
			ex.printStackTrace();
		}
	}

	/**
	 * This method generates a "fake" country for a given IP Address
     */
    private static String getCountryNameFromIpAddress(String ipAddress) {

		String country = null;		

		if(ipAddress != null && !ipAddress.isEmpty())
		{
			int randomIndex = Math.abs(ipAddress.hashCode()) % COUNTRIES.length;
			country = COUNTRIES[randomIndex];

		} 

		return country;
	}
}

