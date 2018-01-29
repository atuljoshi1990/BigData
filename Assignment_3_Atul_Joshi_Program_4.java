import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class Atul_Joshi_Program_4 {

	//Mapper for ACS dataset
	public static class ACSMapper extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			if (value.toString().length() > 0) {
				//Splits each line by ","
				String stringSplit[] = value.toString().split(",");
				//This condition is to skip the first two rows of the data set.
				if (stringSplit[0].startsWith("04")) {
					context.write(new Text(stringSplit[0]), new Text(
							"ACS\t" + stringSplit[3]));
				}
			}
		}
	}

	//Mapper for CFS dataset
	public static class CFSMapper extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			if (value.toString().length() > 0) {
				//Splits each line by ","
				String stringSplit[] = value.toString().split(",");
				//This condition is to skip the first two rows of the data set.
				if (stringSplit[0].startsWith("04")) {
					try{
						//this parsing is done to skip all tons with invalid values.
						int ton = Integer.parseInt(stringSplit[8]);
						context.write(new Text(stringSplit[0]), new Text(
								"CFS\t" + stringSplit[8]));
					}catch(Exception e){
						
					}
				}
			}
		}
	}

	public static class ReduceSideJoinReducer extends
			Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String name = "";
			int total = 0;
			for (Text t : values) {
				String parts[] = t.toString().split("\t");
				//Condition to get the total tons from CFS dataset.
				if (parts[0].equals("CFS")) {
					total += Integer.parseInt(parts[1]);
					//Condition to get population from ACS dataset. 
				} else if (parts[0].equals("ACS")) {
					name = new String(parts[1]);
				}
			}
			//wrtie population and total tons to the output.
			context.write(new Text(name), new Text(Integer.toString(total)));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://hadoop1:9000");
		Job newJob = Job.getInstance(conf, "Reduce_Side_Join");
		newJob.setJarByClass(Atul_Joshi_Program_4.class);
		newJob.setReducerClass(ReduceSideJoinReducer.class);
		String ACSPath = "";
		String CFSPath = "";
		newJob.setOutputKeyClass(Text.class);
		newJob.setOutputValueClass(Text.class);
		FileInputFormat.setInputDirRecursive(newJob, true);
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
		//saves the output in a sequence file format.
		newJob.setOutputFormatClass(SequenceFileOutputFormat.class);
		MultipleInputs.addInputPath(newJob, new Path(ACSPath),
				TextInputFormat.class, ACSMapper.class);
		MultipleInputs.addInputPath(newJob, new Path(CFSPath),
				TextInputFormat.class, CFSMapper.class);
		Path outputPath = new Path(args[2]);
		FileOutputFormat.setOutputPath(newJob, outputPath);
		System.exit(newJob.waitForCompletion(true) ? 0 : 1);
	}
}