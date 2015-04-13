import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class CrimeRate {

	public static void main(String[] args) throws IOException {
		JobConf conf = new JobConf(CrimeRate.class);
		Job job = new Job(conf);
		conf.setJobName("Crime Rate");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(CrimeRateMapper.class);
		conf.setCombinerClass(CrimeRateReducer.class);
		conf.setReducerClass(CrimeRateReducer.class);

		conf.setInputFormat(org.apache.hadoop.mapred.TextInputFormat.class);
		conf.setOutputFormat(org.apache.hadoop.mapred.TextOutputFormat.class);

		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		if (args.length == 4) {
			if (Integer.parseInt(args[2]) != 0) {
				TextInputFormat.setMinInputSplitSize(job,
						Integer.parseInt(args[2]) * 1024 * 1024);
			}
			if (Integer.parseInt(args[3]) != 0) {
				conf.setNumReduceTasks(Integer.parseInt(args[3]));
			}
		}

		JobClient.runJob(conf);

	}
}
