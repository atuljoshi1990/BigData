import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Atul_Joshi_Program_4_Part_2 {
	public static class RatingCountMapPart2 extends
			Mapper<Object, Text, Text, FloatWritable> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String tempString = value.toString();
			String[] newStrng = tempString.split("\t");
			context.write(new Text("Standard Deviation is"),
					new FloatWritable(Float.parseFloat(newStrng[1])));
		}
	}

	public static class RatingCountReducePart2 extends
			Reducer<Text, FloatWritable, Text, FloatWritable> {

		private FloatWritable result = new FloatWritable();

		public void reduce(Text key, Iterable<FloatWritable> values,
				Context context) throws IOException, InterruptedException {
			float count = 0;
			float average = 0f;
			float totalrating = 0f;
			for (FloatWritable val : values) {
				totalrating++;
				count += val.get();
			}
			//calculating average and then standard deviation.
			average = (float)count / totalrating;
			result.set((float)(Math.sqrt(average)));
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration configObj = new Configuration();
		configObj.set("fs.defaultFS", "hdfs://hadoop-master:9000");
		Job newJob = Job.getInstance(configObj, "ratings average part 2");
		newJob.setJarByClass(Atul_Joshi_Program_4_Part_2.class);
		newJob.setMapperClass(RatingCountMapPart2.class);
		newJob.setReducerClass(RatingCountReducePart2.class);
		newJob.setOutputKeyClass(Text.class);
		newJob.setOutputValueClass(FloatWritable.class);
		FileInputFormat.addInputPath(newJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(newJob, new Path(args[1]));
		System.exit(newJob.waitForCompletion(true) ? 0 : 1);
	}
}
