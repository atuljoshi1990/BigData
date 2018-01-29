import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object ATUL_JOSHI_QUESTION_1 {
  def main(args: Array[String]) {
    val configuration = new SparkConf().setMaster("local").setAppName("QUESTION_1")
    val sparkContext = new SparkContext(configuration)
	//get the ratings dataset
	val ratingDataset = sparkContext.textFile("hdfs://hadoop1:9000"+args(0))
	//split the raings dataset.
	val splittedValues = ratingDataset.filter(line => line.contains("::")).map(record => record.split("::"))
	// get the movie and ratings
	val movieRating = splittedValues.map(a=>(a(1),a(2).toDouble))
	//get the movie id and the sum of all of its ratings.
	val sumOfRatings = splittedValues.map(a=>(a(1),a(2).toDouble)).reduceByKey((a,b)=>a+b).distinct
	//get the movie id and get the total number of ratings given to it.
	val totalRatings = splittedValues.map(a=>(a(1),1)).reduceByKey((a,b)=>a+b).distinct
	//join to get movie id, total sum of the ratings and total number of ratings given to it.
	val joinSumAndCount=sumOfRatings.join(totalRatings)
	//calculate average rating for each movie.
	val avgRatingForAMovie=joinSumAndCount.map(a=>(a._1,a._2._1/a._2._2))
	//join to get movie id, average rating for that movie and all the ratings for that movie.
	val joinTables = avgRatingForAMovie.join(movieRating)
	val avgSqr = joinTables.map(a=>((a._1,(a._2._2-a._2._1)*(a._2._2-a._2._1))))
	//calculate variance of the movie's ratings.
	val sumOfAvgSqr = avgSqr.map(a=>(a._1,a._2.toDouble)).reduceByKey((a,b)=>a+b).distinct
	val meanJoin = totalRatings.join(sumOfAvgSqr)
	//calculate standard deviation.
	val standardDeviation = meanJoin.map(a=>(a._1,Math.sqrt(a._2._2/a._2._1)))
	standardDeviation.saveAsTextFile("hdfs://hadoop1:9000"+args(1))
	sparkContext.stop()
  }
}
