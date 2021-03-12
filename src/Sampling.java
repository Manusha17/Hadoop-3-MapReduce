import java.io.IOException;
import java.util.Random;
import java.io.File;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Sampling {

	public static class EmployeeSampling extends Mapper<Object, Text, NullWritable, Text> {
		private Random randomNumber = new Random();
		private Double samplingPercentage;

		protected void setup(Context context) throws IOException, InterruptedException {
			String percentage = context.getConfiguration().get("sampling_percentage");
			samplingPercentage = Double.parseDouble(percentage) / 100.0;
		}

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			if (randomNumber.nextDouble() < samplingPercentage) {
				context.write(NullWritable.get(), value);
			}
		}
	}

	

		public static void main(String[] args) throws Exception {

			args = new String[] { "C:\\hadoop-3.1.0\\datafile\\input\\Employee.txt",
					"C:\\hadoop-3.1.0\\datafile\\SamplingOutput" };

			/* delete the output directory before running the job */
			FileUtils.deleteDirectory(new File(args[1]));

			if (args.length != 2) {
				System.err.println("Please specify the input and output path");
				System.exit(-1);
			}

			Configuration conf = new Configuration();
			conf.set("sampling_percentage", "20");
			Job job = Job.getInstance(conf);
			job.setJarByClass(Sampling.class);
			job.setJobName("Sampling_Employees");
			job.setMapperClass(EmployeeSampling.class);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
			//job.setNumReduceTasks(0);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			System.exit(job.waitForCompletion(true) ? 0 : 1);

		}
	}
