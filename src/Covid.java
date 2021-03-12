import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Covid {
	public static class CovidMapper extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] cols = line.split(",");

			String country = cols[0];
			context.write(new Text(country), new Text(value));

		}
	}

	public static class CovidReducer extends Reducer<Text, Text, Text, Text> {

		public int max = 0;

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			max = 0;
			String date = "";
			for (Text value : values) {
				String line = value.toString();

				String[] tokens = line.split(",");

				if (!tokens[3].equals("") && Integer.parseInt(tokens[3]) > max) {
					max = Integer.parseInt(tokens[3]);
					date = tokens[2];
				}

			}
			context.write(new Text(key), new Text(max + "	" + date));
		}

	}

	public static void main(String[] args) throws Exception {

		args = new String[] { "C:\\hadoop-3.1.0\\datafile\\input\\dataset.csv",
				"C:\\hadoop-3.1.0\\datafile\\CovidOutput" };

		/* delete the output directory before running the job */
		FileUtils.deleteDirectory(new File(args[1]));

		if (args.length != 2) {
			System.err.println("Please specify the input and output path");
			System.exit(-1);
		}

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJobName("Covid");
		job.setJarByClass(Covid.class);
		job.setMapperClass(CovidMapper.class);
		job.setReducerClass(CovidReducer.class);
//		job.setInputFormatClass(TextInputFormat.class);
//		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
