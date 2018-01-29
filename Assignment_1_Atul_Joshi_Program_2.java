import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
public class Atul_Joshi_Program_2 {
 public static void main(String[] args) throws IOException {
  String inputPath = "";
  String outputPath = "";
  String numberOfFiles = "";
  int fileCount = 0;
  int count = 0;
  inputPath = args[0]; // input path to access files
  outputPath = args[1]; // output path to copy files to
  numberOfFiles = args[2]; // number of files to copy
  try {
   fileCount = Integer.parseInt(numberOfFiles);
   //configuration object created to set the hadoop configuration.
   Configuration conf = new Configuration();
   conf.set("fs.defaultFS", "hdfs://hadoop-master:9000"); // set configuration to access hadoop
   FileSystem fs = FileSystem.get(conf);
   FileStatus[] status = fs.listStatus(new Path(inputPath)); //getting the files from hadoop system.
   count = status.length;
   if (fs.exists(new Path(inputPath))) { //check whether input path exists or not.
    if (!fs.exists(new Path(outputPath))) { //check whether output path exists or not.
     if (count > fileCount) { //check whether files to be copied are greater then the files present in hadoop.
      Path finalOutputPath = new Path(outputPath);

      fs.mkdirs(finalOutputPath); //making new output directory
      Path[] pathArray = new Path[fileCount];
      for (int i = 0; i < fileCount; i++) {
       Path path = status[i].getPath();
       String stringPath = path.getName();
       int index = stringPath.lastIndexOf("\\");
       String fileName = stringPath.substring(index + 1);
       String finalInputPath = inputPath + "/" + fileName;
       pathArray[i] = new Path(finalInputPath);
       //copy files to the output path.
       FileUtil.copy(fs, pathArray[i], fs,
        finalOutputPath, false, conf);
      }
      System.out.println("All files copied !!");
     } else {
      System.out
       .println("Error :: Files to copy exceeds the number of files in the system.");
     }
    } else {
     System.out
      .println("Error :: Output directory already exist.");
    }
   } else {
    System.out.println("Error :: Input directory doesn't exist.");
   }
  } catch (Exception e) {
   System.out
    .println("Error :: Input type for number of files is incorrect. :: " + e.getMessage()); // Exception caught if the input count is not proper integer type.
  }
 }
}