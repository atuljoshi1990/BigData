import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.mutable.ListBuffer

object ATUL_JOSHI_QUESTION_4 {
  def main(args: Array[String]) {
        val configuration = new SparkConf().setMaster("local").setAppName("QUESTION_4")
        val sc = new SparkContext(configuration)
        val twitterData = sc.textFile("hdfs://hadoop1:9000"+args(0))
        val output = args(1)
        val listUsers = new ListBuffer[String]
        val splittedValues = twitterData.filter(line => !line.contains("N/A")).map(record => record.split(","))
        val verticesOne = splittedValues.map(a=>{var x=0; for(c<-(a(1).split("\t"))(0)) x=x+c.toInt;(x.toLong,(a(1).split("\t"))(0))})
        val verticesTwo = splittedValues.map(a=>{var y=0; for(c<-(a(1).split("\t"))(1)) y=y+c.toInt;(y.toLong,(a(1).split("\t"))(1))})
        val vertices = verticesOne.union(verticesTwo)
        val edges = splittedValues.map(a => {var x=0; var y=0; for(c<-(a(1).split("\t"))(0)) x=x+c.toInt; for(c<-(a(1).split("\t"))(1)) y=y+c.toInt; Edge(x.toLong,y.toLong,"follows");})
        EdgeRDD.fromEdges(edges)
        val graph = Graph(vertices, edges)
        //graph
        graph.vertices.saveAsTextFile("hdfs://hadoop1:9000"+output+"_1")
        graph.edges.saveAsTextFile("hdfs://hadoop1:9000"+output+"_2")
        //Influence

        def sendMsg(ec: EdgeContext[Int,String,Int]): Unit =
        {
        ec.sendToDst(ec.srcAttr+1)
        }

        def mergeMsg(a: Int, b: Int): Int =
        {
        math.max(a,b)
        }

        def propagateEdgeCount(g:Graph[Int,String]):Graph[Int,
        String] =
        {
        val verts = g.aggregateMessages[Int](sendMsg,
        mergeMsg)
        val g2 = Graph(verts, g.edges)
        val check = g2.vertices.join(g.vertices). map(x =>x._2._1 - x._2._2). reduce(_ + _)
        if (check > 0) propagateEdgeCount(g2) else g
        }
        val initialGraph = graph.mapVertices((_,_) => 0)
        val newgraph = Graph(initialGraph.vertices, initialGraph.edges.reverse)
        propagateEdgeCount(newgraph).vertices.saveAsTextFile("hdfs://hadoop1:9000"+output+"_3")

        //page rank of each vertex
        val ranks = graph.pageRank(0.0001).vertices
        ranks.saveAsTextFile("hdfs://hadoop1:9000"+output+"41")

        //recommend  one user for every other user

        graph.vertices.collect.foreach(x =>
        {
        val users =  graph.personalizedPageRank(x._1.toLong, 0.0001).vertices.filter(_._1.toLong != x._1.toLong).reduce((a,b) => if(a._2 > b._2) a else b)
        listUsers += x._2 + "|" + users._2
        })
        sc.parallelize(listUsers).saveAsTextFile("hdfs://hadoop1:9000"+output+"_5")
        //strongly connected users
        graph.stronglyConnectedComponents(10).vertices.map(_.swap).groupByKey.map(_._2).saveAsTextFile("hdfs://hadoop1:9000"+output+"_6")
        //User communities
		sc.stop()
        }
	}