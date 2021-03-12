import java.io.File;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Summation {
	public static class GlobalNumberAdditionMapper extends Mapper<Object, Text, Text, IntWritable> {

		int sum = 0;

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			StringTokenizer itr = new StringTokenizer(value.toString());

			while (itr.hasMoreTokens()) {

				String str = itr.nextToken();

				sum = sum + Integer.parseInt(str);

			}

		}

		public void cleanup(Context context) throws IOException, InterruptedException {

			context.write(new Text("Addition of numbers is"), new IntWritable(sum));

		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Addition of Numbers");
		job.setJarByClass(Summation.class);
		job.setMapperClass(GlobalNumberAdditionMapper.class);
//		job.setCombinerClass(IntSumReducer.class);
//		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		args = new String[] { "C:\\hadoop-3.1.0\\datafile\\input\\numbers.txt",
				"C:\\hadoop-3.1.0\\datafile\\totaloutput" };
		/* delete the output directory before running the job */
		FileUtils.deleteDirectory(new File(args[1]));
		if (args.length != 2) {
			System.err.println("Please specify the input and output path");
			System.exit(-1);
		}
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
