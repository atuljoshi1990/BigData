import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object ATUL_JOSHI_QUESTION_2 {
  def main(args: Array[String]) {
    val configuration = new SparkConf().setMaster("local").setAppName("QUESTION_2")
    val sparkContext = new SparkContext(configuration)
	//Get the movie id
    val input = readLine("Give the movieid !! ")
		//get the ratings dataset.
        val ratingDataset = sparkContext.textFile("hdfs://hadoop1:9000"+args(0))
		//filter the dataset for the movie id which came as input.
        val ratingWithAperticularMovieId = ratingDataset.filter(line => line.contains("::"+input+"::")).map(record => record.split("::"))
		//calculate the sum of all the ratings for that movie id.
        val sumOfRatings = ratingWithAperticularMovieId.map(a=>(a(1),a(2).toDouble)).reduceByKey((a,b)=>a+b).distinct
		//calculate total number of ratings given to that movie.
        val totalRatings = ratingWithAperticularMovieId.map(a=>(a(1),1)).reduceByKey((a,b)=>a+b).distinct
		//join to get movie id, total sum of the ratings and total number of ratings given.
        val joinSumAndCount=sumOfRatings.join(totalRatings)
		//calculate the average.
        val avgRatingForAMovie=joinSumAndCount.map(a=>(a._1,a._2._1/a._2._2))
        avgRatingForAMovie.saveAsTextFile("hdfs://hadoop1:9000"+args(1))
        sparkContext.stop()
  }
}
