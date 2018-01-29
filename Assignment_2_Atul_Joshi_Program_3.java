import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Atul_Joshi_Program_3 extends Configured implements Tool {
	public static class RatingsCountMap extends
			Mapper<Object, Text, Text, IntWritable> {

		private final static Text movieIdParam = new Text();

		// Overrided setup method of the Mapper to get the configuration and thus the parameter. 
		public void setup(Context context) {
			Configuration config = context.getConfiguration();
			// gets the parameter passed from command line.
			String wordstring = config.get("movieId");
			movieIdParam.set(wordstring);
		}

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String movieId = "";
			movieId = movieIdParam.toString();
			String tempString = value.toString();
			String[] newStrng = tempString.split("::");
			if (movieId.equals(newStrng[1])) {
				context.write(new Text(
						"::::::::: Average movie rating for movie with movieID["
								+ movieId + "]  :::::::"), new IntWritable(
						Integer.parseInt(newStrng[2])));
			}
		}
	}

	public static class RatingsCountReduce extends
			Reducer<Text, IntWritable, Text, FloatWritable> {

		private FloatWritable result = new FloatWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int count = 0;
			float average = 0f;
			float totalMovies = 0f;
			// calculate total number of movies
			for (IntWritable val : values) {
				totalMovies++;
				count += val.get();
			}
			// calculate average
			average = count / totalMovies;
			result.set(average);
			context.write(key, result);
		}
	}
	//implemeted ToolRunner class to fetch the parameters from the command line.
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new Atul_Joshi_Program_3(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		Configuration configObj = this.getConf();
		configObj.set("fs.defaultFS", "hdfs://hadoop-master:9000");
		Job newJob = Job.getInstance(configObj, "movie rating average");
		newJob.setJarByClass(Atul_Joshi_Program_3.class);
		newJob.setMapperClass(RatingsCountMap.class);
		newJob.setReducerClass(RatingsCountReduce.class);
		newJob.setOutputKeyClass(Text.class);
		newJob.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(newJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(newJob, new Path(args[1]));
		return newJob.waitForCompletion(true) ? 0 : 1;
	}
}
