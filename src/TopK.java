import java.io.File;
import java.io.IOException;
import java.util.TreeMap;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class TopK {
	public static class TopTenMapper extends Mapper<Object, Text, NullWritable, Text> {
		// Stores a map of employees salary’s to the record
		private TreeMap<Float, Text> ToRecordMap = new TreeMap<Float, Text>();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] tokens = line.split(",");
			// split the data and fetch salary
			float salary = Float.parseFloat(tokens[3]);
			// insert salary as key and entire row as value
			// tree map sort the records based on salary
			ToRecordMap.put(salary, new Text(value));
			// If we have more than ten records, remove the one with the lowest salary
			// As this tree map is sorted in ascending order, the employee with
			// the lowest salary is the first key.
			if (ToRecordMap.size() > 5) {
				ToRecordMap.remove(ToRecordMap.lastKey());
			}
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			// Output top ten records to the reducers with a null key
			for (Text t : ToRecordMap.values()) {
				String line = t.toString();
				String[] cols = line.split(",");

				String sal = cols[3];
				context.write(NullWritable.get(), new Text(sal));
			}
		}
	}

//	public static class TopTenReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
//		private TreeMap<Float, Text> TopRecordMap = new TreeMap<Float, Text>();
//
//		public void reduce(NullWritable key, Iterable<Text> values, Context context)
//				throws IOException, InterruptedException {
//			for (Text value : values) {
//				String line = value.toString();
//				if (line.length() > 0) {
//					String[] tokens = line.split(",");
//					// split the data and fetch salary
//					float salary = Float.parseFloat(tokens[3]);
//					// insert salary as key and Salary as value
//					// tree map sort the records based on salary
//					TopRecordMap.put(salary, new Text(String.valueOf(salary)));
//				}
//			}
//			// If we have more than ten records, remove the one with the lowest salary
//			// As this tree map is sorted in ascending order, the user with
//			// the lowest salary is the first key.
//			if (TopRecordMap.size() > 5) {
//				TopRecordMap.remove(TopRecordMap.lastKey());
//			}
//			for (Text t : TopRecordMap.descendingMap().values()) {
//				// Output our ten records to the file system with a null key
//				context.write(NullWritable.get(), t);
//			}
//		}
//	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "TopK");
		job.setJarByClass(TopK.class);
		job.setMapperClass(TopTenMapper.class);
//		job.setCombinerClass(IntSumReducer.class);
//		job.setReducerClass(TopTenReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		args = new String[] { "C:\\hadoop-3.1.0\\datafile\\input\\topk.txt",
				"C:\\hadoop-3.1.0\\datafile\\TopKoutput" };
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
