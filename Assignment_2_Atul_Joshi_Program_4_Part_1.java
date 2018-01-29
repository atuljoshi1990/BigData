import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Atul_Joshi_Program_4_Part_1 {
	public static class RatingCountMap extends
			Mapper<Object, Text, Text, IntWritable> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String tempString = value.toString();
			String[] newStrng = tempString.split("::");
				context.write(new Text("rating"), new IntWritable(Integer.parseInt(newStrng[2])));
		}
	}

	public static class RatingCountReduce extends
			Reducer<Text, IntWritable, Text, FloatWritable> {

		private FloatWritable result = new FloatWritable();
		ArrayList<Integer> ratingList = new ArrayList<Integer>();
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int count = 0;
			float average = 0f;
			float totalrating = 0f;
			//calculate total rating.
			for (IntWritable val : values) {
				ratingList.add(val.get());
				totalrating++;
				count += val.get();
			}
			//total average 
			average = count / totalrating;
			for(Integer rating : ratingList){
				//calculating the variance.
				result.set((float)((rating-average)*(rating-average)));
				context.write(new Text(rating.toString()), result);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration configObj = new Configuration();
		configObj.set("fs.defaultFS", "hdfs://hadoop-master:9000");
		Job newJob = Job.getInstance(configObj, "ratings average");
		newJob.setJarByClass(Atul_Joshi_Program_4_Part_1.class);
		newJob.setMapperClass(RatingCountMap.class);
		newJob.setReducerClass(RatingCountReduce.class);
		newJob.setOutputKeyClass(Text.class);
		newJob.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(newJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(newJob, new Path(args[1]));
		System.exit(newJob.waitForCompletion(true) ? 0 : 1);
	}
}
