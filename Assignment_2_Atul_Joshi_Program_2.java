import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

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

public class Atul_Joshi_Program_2 {
	public static class GenersCountMap extends
			Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String tempString = value.toString();
			String[] newStrng = tempString.split("::");
			//splits the geners with multiple values seperated by |
			String[] geners = newStrng[2].split("\\|");
			for (int i = 0; i < geners.length; i++) {
				context.write(new Text(geners[i]), one);
			}
		}
	}

	public static class GenersCountReduce extends
			Reducer<Text, IntWritable, Text, FloatWritable> {

		private Map<Text, Float> countMap = new HashMap<Text, Float>();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			float count = 0f;
			for (IntWritable val : values) {
				count += val.get();
			}
			//put the key and the number of occurrences to a map.
			countMap.put(new Text(key), count);
		}
		//This is a Reducer method which is overrided here to perform collection sorting on the data.
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			
			Map<Text, Float> sortedMap = sortByKeys(countMap);
			float average = 0f;
			float totalMovies = 0f;
			for (Text key : sortedMap.keySet()) {
				totalMovies = totalMovies + sortedMap.get(key);
			}
			for (Text key : sortedMap.keySet()) {
				average = sortedMap.get(key)/totalMovies;
				context.write(key, new FloatWritable(average));
			}
		}
		
		//method to perform sorting on the input data.
		private Map<Text, Float> sortByKeys(
				Map<Text, Float> countMap) {
			
			List<Text> keys = new LinkedList<Text>(countMap.keySet());
		     
	        Collections.sort(keys, Collections.reverseOrder());
	     
	        //LinkedHashMap will keep the keys in the order they are inserted
	        //which is currently sorted on natural ordering
	        Map<Text,Float> sortedMap = new LinkedHashMap<Text,Float>();
	     
	        for(Text key: keys){
	            sortedMap.put(key, countMap.get(key));
	        }
	        return sortedMap;
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration configObj = new Configuration();
		configObj.set("fs.defaultFS", "hdfs://hadoop-master:9000");
		Job newJob = Job.getInstance(configObj, "geners average");
		newJob.setJarByClass(Atul_Joshi_Program_2.class);
		newJob.setMapperClass(GenersCountMap.class);
		newJob.setReducerClass(GenersCountReduce.class);
		newJob.setOutputKeyClass(Text.class);
		newJob.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(newJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(newJob, new Path(args[1]));
		System.exit(newJob.waitForCompletion(true) ? 0 : 1);
	}
}
