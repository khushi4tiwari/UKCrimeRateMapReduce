import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;


public class CrimeRateMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>{

	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		try { 
		String[] crime = value.toString().split(",");
		String region = crime[4].substring(0,3)+","+crime[5].substring(0, 3)+":"+crime[7]; 
		output.collect(new Text(region), new IntWritable(1));
		}
		catch(IndexOutOfBoundsException e){
		}
		catch(NullPointerException e){
		}
	}

}
