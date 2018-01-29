import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object ATUL_JOSHI_QUESTION_3 {
  def main(args: Array[String]) {
	val configuration = new SparkConf().setMaster("local").setAppName("QUESTION_3")
	val sparkContext = new SparkContext(configuration)
	var input1 = ""
	var input2 = ""
	if(args(0).contains("ACS")){
			input1 = args(0)
			input2 = args(1)
	}else if(args(0).contains("CFS")){
			input1 = args(1)
			input2 = args(0)
	}
	if (input1.endsWith("/")) {
			input1 = input1.dropRight(1)
	}
	if (input2.endsWith("/")) {
			input2 = input2.dropRight(1)
	}
	//get the acs dataset.
	val acsSmallDataset = sparkContext.textFile("hdfs://hadoop1:9000"+input1+"/ACS_15_5YR_S0101_with_ann.csv")
	//get the cfs dataset.
	val cfsLargeDataset = sparkContext.textFile("hdfs://hadoop1:9000"+input2+"/CFS_2012_00A01_with_ann.csv")
	//filterout the header values from both the datasets.
	val acsFilteredDataset = acsSmallDataset.filter(line => line.contains("0400000US")).map(record => record.split(","))
	val cfsFilteredDataset = cfsLargeDataset.filter(line => line.contains("0400000US")).map(record => record.split(","))
	//remove the scrap data from the CFS dataset.
	val removedUnwantedData = cfsFilteredDataset.filter(a => !a(8).contains("S") && !a(8).contains("X") && !a(8).contains("Z"))
	val govIdPopulation = acsFilteredDataset.map(a=>(a(0),a(3).toDouble))
	val govIdTotalTons = removedUnwantedData.map(a=>(a(0),a(8).toDouble))
	get the total tons for a state.
	val totalTonsForAGovID = govIdTotalTons.map(a=>(a._1,a._2)).reduceByKey((a,b)=>a+b).distinct
	val joinTwoDatasets=govIdPopulation.join(totalTonsForAGovID)
	// get total population and total tons for a state.
	val populationAndTotalTons=joinTwoDatasets.map(a=>(a._2._1,a._2._2))
	populationAndTotalTons.saveAsSequenceFile("hdfs://hadoop1:9000"+args(2))
	sparkContext.stop()
  }
}
