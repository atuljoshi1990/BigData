#input path from the command line
inputPath="$1"
#output path from the command line
outputPath="$2"
#number of files to be copied.
count="$3"
#condition to check whether the input path exist or not.
if hadoop fs -test -d  $inputPath ;
then
echo "Input directory exists."
else
echo  "Error : Input directory doesn't exist."
exit 0
fi
#condition to check whether the output path exist or not.
if hadoop fs -test -d  $outputPath ;
then
echo "Error : Output directory already exists."
exit 0
else
echo  "Output directory doesn't exist."
#if output path doesn't exist create a new output directory.
hdfs dfs -mkdir $outputPath
echo "New directory created successfully."
fi
#getting the number files in hadoop file system.
n=$(hadoop fs -ls $inputPath/* | wc -l)
#condition to check whether the number of files in hadoop file system are less then the number of files to copy.
if(( $count>$n )); then
echo "Error : Number of files to be copied requested are more then they exist."
exit 0
else
for i in $(hadoop fs -stat "%n" $inputPath/*); do
  [ $((count--)) = 0 ] && break
#copying the files to the output directory.
  hadoop fs -cp $inputPath/$i $outputPath
done
echo "All files copied !!"
fi
