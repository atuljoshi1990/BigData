import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object ATUL_JOSHI_QUESTION_5 {
  def main(args: Array[String]) {
    val configuration = new SparkConf().setMaster("local[2]").setAppName("QUESTION_5")
	val ssc = new StreamingContext(configuration, Seconds(1))
	val lines = ssc.socketTextStream("localhost", 11000)
	val words = lines.flatMap(_.split(" "))
	val pairs = words.map(word =>{
	var value = ""
		if(word(0).toInt%2 == 1){
			if(word.length()%2 == 0){
					value = " :: ODD :: EVEN LENGTH";
			}else{
					value = " :: ODD :: ODD LENGTH"
			}
		}else{
			if(word.length()%2 == 0){
					value = " :: EVEN :: EVEN LENGTH";
			}else{
					value = " :: EVEN :: ODD LENGTH";
			}
		}
		(word, value)
	})  
	pairs.print()
	pairs.saveAsTextFiles("hdfs://hadoop1:9000"+args(0))
    ssc.start()
	ssc.awaitTermination()
    
  }
}
