import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object ATUL_JOSHI_QUESTION_2 {
  def main(args: Array[String]) {
	val configuration = new SparkConf().setMaster("local").setAppName("QUESTION_2")
	val sc = new SparkContext(configuration)
	val moviesDataset = sc.textFile("hdfs://hadoop1:9000/CS5433/PA2/movies.dat")
	moviesDataset.cache()
	val ratingsDataset = sc.textFile("hdfs://hadoop1:9000/CS5433/PA2/ratings.dat")
	val splittedRatingValues = ratingsDataset.filter(line => line.contains("::")).map(record => record.split("::"))
	val splittedMoviesValues = moviesDataset.filter(line => line.contains("::")).map(record => record.split("::"))
	val movieTitle = splittedMoviesValues.map(a=>(a(0),a(1)))
	val movieRatings = splittedRatingValues.map(a=>(a(1),a(2).toDouble))
	val movieWithLeastRating = movieRatings.map(a=>(a._1,a._2)).reduceByKey((a,b)=>if (a<b) a else b).distinct
	val joinTwoDatasets=movieTitle.join(movieWithLeastRating)
	val movieTitleAndItsLeastRating=joinTwoDatasets.map(a=>(a._2._1,a._2._2))
	val sortByRatings = movieTitleAndItsLeastRating.sortBy(_._2)
	val descSortedData = sortByRatings.sortByKey(false)
	descSortedData.saveAsTextFile("hdfs://hadoop1:9000"+args(0))
	sc.stop()
  }
}
