#input path from command line
inputPath="$1"
#output path from command line.
outputPath="$2"
#number of files to copy.
count="$3"
#a variable for comparing the number files to copy and files present in hadoop.
newCount="$3"
#counter to create output directory dynamically.
pathCounter=0
#counters to skip copying the files which have already been copied.
skipCopy=0
checkSkipCopy=0
#get number of files in hadoop file system.
n=$(hadoop fs -ls $inputPath/* | wc -l)
count=$((n))
cpd=0
#check input path ecist or not.
if hadoop fs -test -d  $inputPath ;
then
echo "Input directory exists."
else
echo  "Error : Input directory doesn't exist."
exit 0
fi
#check output path exist or not.
if hadoop fs -test -d  $outputPath ;
then
echo "Error : Output directory already exists."
exit 0

else
echo  "Output directory doesn't exist."
#create new output directroy if doesn't exist.
hdfs dfs -mkdir $outputPath
fi
#check number of files in hadoop system are greater then the files to be copied.
if(( $newCount>$n )); then
echo "Error : Number of files to be copied requested are more then they exist."
exit 0
else
while $(true); do
cpd=0
#increamenting the path counter to make new sub output directory.
pathCounter=$((pathCounter+1))
#appending the counter to the output path.
newDirec=$outputPath/$pathCounter
hdfs dfs -mkdir $newDirec
echo "Creating new Dir : $newDirec"
checkSkipCopy=0
for i in $(hadoop fs -stat "%n" $inputPath/*); do
  [ $((count--)) = 0 ] && break
#skipping copy if the files have already been copied.
        if (( $skipCopy>$checkSkipCopy )); then
                 checkSkipCopy=$((checkSkipCopy+1))
        else
#copying new files
                hadoop fs -cp $inputPath/$i $newDirec
                cpd=$((cpd+1))
        fi
        if (( $cpd==$3 )); then
                break
        fi

done
        skipCopy=$((skipCopy+$3))

done
echo "All files copied !!"
fi
