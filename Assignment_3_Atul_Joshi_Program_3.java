import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

@SuppressWarnings("deprecation")
public class Atul_Joshi_Program_3 {

	public static class MapperJoin extends
			Mapper<Object, Text, Text, IntWritable> {

		private String totalPopulation = "";
		private Text outputKey = new Text();
		private IntWritable outputValue = new IntWritable();
		private BufferedReader bufferedReader;
		private static Map<String, String> joinMap = new HashMap<String, String>();

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {

			String lineOfFile = null;
			try {
				//read the file from cache
				bufferedReader = new BufferedReader(new FileReader(new File(
						"ACS_15_5YR_S0101_with_ann.csv").toString()));
				//put the gov.id and its population in a map
				while ((lineOfFile = bufferedReader.readLine()) != null) {
					String stringSplit[] = lineOfFile.split(",");
					joinMap.put(stringSplit[0], stringSplit[3]);
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				if (null != bufferedReader) {
					bufferedReader.close();
				}
			}
		}

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			try {
				if (value.toString().length() > 0) {
					String stringSplit[] = value.toString().split(",");
					if (stringSplit[0].startsWith("04")) {
						//join using the gov_id as key.
						totalPopulation = joinMap
								.get(stringSplit[0].toString());
						outputKey.set(totalPopulation);
						outputValue.set(Integer.parseInt(stringSplit[8]));
						context.write(outputKey, outputValue);
					}
				}
				totalPopulation = "";
			} catch (Exception e) {

			}
		}
	}

	public static class TonsCountReduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		private Map<String, String> map = new HashMap<String, String>();

		private BufferedReader bufferedReader;
		private Map<String, Integer> reduceMap = new HashMap<String, Integer>();

		protected void cleanup(Context context) throws IOException,
				InterruptedException {
				
			IntWritable result = new IntWritable();
			String popKey = "";
			for (Map.Entry entry : map.entrySet()) {
				popKey = entry.getKey().toString();
				if (reduceMap.containsKey(popKey)) {
					//check all the states from CFS datasets.
					result.set(reduceMap.get(popKey));
					context.write(new Text(popKey), result);
				} else {
					//if all states are from ACS dataset are not present in CFS dataset assign 0 value to their tons.
					context.write(new Text(popKey), new IntWritable(0));
				}
			}
		}

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int count = 0;
			for (IntWritable val : values) {
				count = count + val.get();
			}
			//read again from cache to get all those states whose data is not present in CFS dataset.
			reduceMap.put(key.toString(), count);
			bufferedReader = new BufferedReader(new FileReader(new File(
					"ACS_15_5YR_S0101_with_ann.csv").toString()));
			String lineOfFile = null;
			// Read each line, split and load to HashMap
			while ((lineOfFile = bufferedReader.readLine()) != null) {
				String stringSplit[] = lineOfFile.split(",");
				if (stringSplit[0].startsWith("04")) {
					map.put(stringSplit[3], "1");
				}
			}
		}

	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://hadoop1:9000");
		Job newJob = Job.getInstance(conf, "Map_Side_Join");
		newJob.setJarByClass(Atul_Joshi_Program_3.class);
		newJob.setMapperClass(MapperJoin.class);
		String ACSPath = "";
		String CFSPath = "";
		//check whether the input path is of CFS or ACS
		if (args[0].contains("ACS")) {
			ACSPath = args[0];
			CFSPath = args[1];
		} else if (args[0].contains("CFS")) {
			ACSPath = args[1];
			CFSPath = args[0];
		}
		if (ACSPath.endsWith("/")) {
			ACSPath = ACSPath.substring(0, ACSPath.length() - 1);
		}
		if (CFSPath.endsWith("/")) {
			CFSPath = CFSPath.substring(0, CFSPath.length() - 1);
		}
		FileInputFormat.setInputPaths(newJob, new Path(CFSPath));
		FileOutputFormat.setOutputPath(newJob, new Path(args[2]));
		FileOutputFormat.setCompressOutput(newJob, true);
		FileInputFormat.setInputDirRecursive(newJob, true);
		//put the small dataset file in cache
		DistributedCache.addCacheFile(new URI(ACSPath+"/ACS_15_5YR_S0101_with_ann.csv"),
				newJob.getConfiguration());
		newJob.setReducerClass(TonsCountReduce.class);
		newJob.setOutputKeyClass(Text.class);
		newJob.setOutputValueClass(IntWritable.class);
		System.exit(newJob.waitForCompletion(true) ? 0 : 1);
	}
}
