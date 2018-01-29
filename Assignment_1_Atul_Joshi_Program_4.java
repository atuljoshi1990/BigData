import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import
org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
public class Atul_Joshi_Program_4 {
 public static void main(String[] args) throws IOException {
  String inputPath = "";
  String outputPath = "";
  String numberOfFiles = "";
  String tempPath = "";
  int fileCount = 0;
  int count = 0;
  inputPath = args[0]; // input path to access files
  tempPath = args[1]; // output path to copy files to
  numberOfFiles = args[2]; // number of filed to copy
  int pathCounter = 0;
  int fileNameCount = 0;
  try {
   fileCount = Integer.parseInt(numberOfFiles);
   //configuration object created to set the hadoop configuration.
   Configuration conf = new Configuration();
   conf.set("fs.defaultFS", "hdfs://hadoop-master:9000"); // set configuration to access hadoop
   FileSystem fs = FileSystem.get(conf);
   //getting the files from hadoop system.
   FileStatus[] status = fs.listStatus(new Path(inputPath));
   count = status.length;
   if (fs.exists(new Path(inputPath))) { //check whether input path exists or not.
    if (!fs.exists(new Path(tempPath))) { //check whether output path exists or not.
     if (count > fileCount) { //check whether files to be copied are greater then the files present in hadoop.
      while (true) {
       if (count > 0) {
        pathCounter = pathCounter + 1; //path counter to add count to the output directory
        outputPath = tempPath + "/" + pathCounter;
        Path finalOutputPath = new Path(outputPath);
        fs.mkdirs(finalOutputPath); //making new output directory
        System.out.println("Created new Directory. :: " + finalOutputPath);
        Path[] pathArray = new Path[fileCount];
        try {
         for (int i = 0; i < fileCount; i++) {
          Path path = status[i + fileNameCount].getPath();
          String stringPath = path.getName();
          int index = stringPath.lastIndexOf("\\");
          String fileName = stringPath
           .substring(index + 1);
          String finalInputPath = inputPath + "/" + fileName;
          pathArray[i] = new Path(finalInputPath);
          //copy files to the output path.
          FileUtil.copy(fs, pathArray[i], fs, finalOutputPath, false, conf);
         }
         count = count - fileCount; // manipulating count value to create sub folders.
        } catch (Exception e) {
         System.out.println("All files copied !!");
         break;
        }
       } else {
        System.out.println("All files copied !!");
        break;
       }
       fileNameCount = fileNameCount + fileCount; //fileNameCounter to skip all the files which have been already copied.
      }
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