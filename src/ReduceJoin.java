import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ReduceJoin {
	public static class EmpMapper extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String record = value.toString();
			String[] parts = record.split("\t", -3);
			context.write(new Text(parts[0]), new Text("emp   " + parts[1]));
		}
	}

	public static class DeptMapper extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String record = value.toString();
			String[] parts = record.split("\t", -3);
			context.write(new Text(parts[2]), new Text("dept   " + parts[3]));
		}
	}

	public static class ReduceJoinReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String name = "";
			double total = 0.0;
			int count = 0;
			for (Text t : values) {
				String parts[] = t.toString().split("  ");
				if (parts[0].equals("dept")) {
					count++;
					total += Float.parseFloat(parts[1]);
				} else if (parts[0].equals("emp")) {
					name = parts[1];
				}
			}
			String str = String.format("%d %f", count, total);
			if (count != 0)		// for inner join										
				context.write(new Text(name), new Text(str));
		}
	}

	public static void main(String[] args) throws Exception {

		args = new String[] { "C:\\hadoop-3.1.0\\datafile\\input\\Employee.txt",
				"C:\\hadoop-3.1.0\\datafile\\input\\Department.txt", "C:\\hadoop-3.1.0\\datafile\\ReduceJoinOutput" };

		/* delete the output directory before running the job */
		FileUtils.deleteDirectory(new File(args[2]));

		if (args.length != 3) {
			System.err.println("Please specify the input and output path");
			System.exit(-1);
		}

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJobName("Reduce-side join");
		job.setJarByClass(ReduceJoin.class);
		job.setReducerClass(ReduceJoinReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, EmpMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, DeptMapper.class);
		Path outputPath = new Path(args[2]);

		FileOutputFormat.setOutputPath(job, outputPath);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}