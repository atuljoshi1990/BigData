import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
object ATUL_JOSHI_QUESTION_3 {

  def main(args: Array[String]) {

    val spark = SparkSession.builder().appName("Q3").config("spark.some.config.option","some-value").getOrCreate()
        import spark.implicits._
        var input1 = ""
        var input2 = ""
        val totalPop = "GOV_ID TOTAL_POPULATION"
        val fields1 = totalPop.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true))
        val totalTons = "GOV_ID TOTAL_TONS"
        val fields2 = totalTons.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true))
        val schema1 = StructType(fields1)
        val schema2 = StructType(fields2)
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
        val acsSmallDataset = spark.sparkContext.textFile("hdfs://hadoop1:9000"+input1+"/ACS_15_5YR_S0101_with_ann.csv")
        //get the cfs dataset.
        val cfsLargeDataset = spark.sparkContext.textFile("hdfs://hadoop1:9000"+input2+"/CFS_2012_00A01_with_ann.csv")
        val acsFilteredDataset = acsSmallDataset.filter(line => line.contains("0400000US")).map(record => record.split(","))
        val govIdPopulation = acsFilteredDataset.map(a=> Row(a(0),a(3).toString))
        val cfsFilteredDataset = cfsLargeDataset.filter(line => line.contains("0400000US")).map(record => record.split(","))
        val removedUnwantedData = cfsFilteredDataset.filter(a => !a(8).contains("S") && !a(8).contains("X") && !a(8).contains("Z"))
        val govIdTotalTons = removedUnwantedData.map(a=>(a(0),a(8).toDouble))
        //get the total tons for a state.
        val totalTonsForAGovID = govIdTotalTons.map(a=>(a._1,a._2)).reduceByKey((a,b)=>(a+b)).distinct
        val totalTonsOfEachGovId = totalTonsForAGovID.map(a=>Row(a._1,a._2.toString))
        val schema1Df = spark.createDataFrame(govIdPopulation, schema1)
        val schema2Df = spark.createDataFrame(totalTonsOfEachGovId, schema2)
        schema1Df.registerTempTable("total_population")
		schema2Df.registerTempTable("total_tons")
        val joined = spark.sql("SELECT a.TOTAL_POPULATION, s.TOTAL_TONS FROM total_population a JOIN total_tons s ON a.GOV_ID = s.GOV_ID")
        joined.show()
        joined.rdd.saveAsTextFile("hdfs://hadoop1:9000"+args(2))
		spark.stop()
  }
}
