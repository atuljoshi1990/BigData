import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Atul_Joshi_Program_1 {
	
	public static class UsersCountMap extends Mapper <Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String tempString = value.toString();
			String[] newStrng = tempString.split("::");
			if("M".equals(newStrng[1])){ //checks condition for male.
				if("25".equals(newStrng[2]) || "35".equals(newStrng[2])){ //condition for age range.
					Text occupationKey = new Text(newStrng[3]);
					context.write(occupationKey, one);
				}
			}
		}
	}

	public static class UsersCountReduce extends Reducer <Text, IntWritable, Text, IntWritable> {

		 private Map<Text, IntWritable> countMap = new HashMap<Text, IntWritable>();
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            // put the key and the number of occurrences to a map.
        		countMap.put(new Text(key), new IntWritable(sum));
        	
            
	}
		//This is a Reducer method which is overrided here to perform collection sorting on the data.
		protected void cleanup(Context context) throws IOException, InterruptedException {

            Map<Text, IntWritable> sortedMap = sortByValues(countMap);

            int counter = 0;
            for (Text key : sortedMap.keySet()) {
            		if (counter++ == 5) {
                        break;
               }
            		context.write(key, sortedMap.get(key));
            }
            context.write(new Text("Total Occupations between 25 to 45 :"), new IntWritable(sortedMap.size()));
        }
		
		//method to perform sorting on the input data.
		private Map<Text, IntWritable> sortByValues(
				Map<Text, IntWritable> countMap2) {
			
			List<Map.Entry<Text,IntWritable>> entries = new LinkedList<Map.Entry<Text,IntWritable>>(countMap2.entrySet());
		     
	        Collections.sort(entries, new Comparator<Map.Entry<Text,IntWritable>>() {

	            @Override
	            public int compare(Entry<Text,IntWritable> objectOne, Entry<Text,IntWritable> objectTwo) {
	                return objectTwo.getValue().compareTo(objectOne.getValue());
	            }
	        });
	     
	        //LinkedHashMap will keep the keys in the order they are inserted
	        //which is currently sorted on natural ordering
	        Map<Text,IntWritable> sortedMap = new LinkedHashMap<Text,IntWritable>();
	     
	        for(Map.Entry<Text,IntWritable> entry: entries){
	            sortedMap.put(entry.getKey(), entry.getValue());
	        }
	     
	        return sortedMap;
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration configObj = new Configuration();
		configObj.set("fs.defaultFS", "hdfs://hadoop-master:9000");
		Job newJob = Job.getInstance(configObj, "user count");
		newJob.setJarByClass(Atul_Joshi_Program_1.class);
		newJob.setMapperClass(UsersCountMap.class);
		newJob.setReducerClass(UsersCountReduce.class);
		newJob.setOutputKeyClass(Text.class);
		newJob.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(newJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(newJob,new  Path(args[1]));
		System.exit(newJob.waitForCompletion(true) ? 0 : 1);	
	}
}