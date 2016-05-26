/* WordCount.scala */
import org.apache.spark._
import org.apache.hadoop.fs._
import java.util.regex._
// import java.util._
import java.lang.Double
import scala.collection.mutable.ListBuffer

object WordCount {
    def main(args: Array[String]) {
        val files = "/shared/HW2/sample-in/input-100M"
        val outputPath = "HW2_Spark_100M"
        val conf = new SparkConf().setAppName("PageRank")
        val sc = new SparkContext(conf)
        
        // Cleanup output dir
        val hadoopConf = sc.hadoopConfiguration
        val hdfs = FileSystem.get(hadoopConf)
        try { hdfs.delete(new Path(outputPath), true) } catch { case _ : Throwable => { } }
        
        val lines = sc.textFile(files)
        val titleReg = "<title>(.+?)</title>"
        val titleSplitReg = "<title>|</title>"
        val originGraph = lines.flatMap(line =>{
          val m = Pattern.compile(titleReg).matcher(line)
          val mNode = Pattern.compile("\\[\\[(.+?)([\\|#]|\\]\\])").matcher(line)
          val isFind = m.find()
          val titleStr = m.group().split(titleSplitReg)
        
          titleStr.filter(_.length() != 0).map(word => {
            val title = word.replaceAll("&quot;", "\"")
                            .replaceAll("&gt;", ">")
                            .replaceAll("&lt;", "<")
                            .replaceAll("&amp;","&")
                            .replaceAll("&apos;", "'")
            var allMatches: ListBuffer[String] = ListBuffer[String]() 
            while (mNode.find()) {
              val patterns = mNode.group().split("[\\[\\]\\|#]+")
              if (patterns.length > 1) {
                val nextNode = patterns(1).replaceAll("&quot;", "\"")
                                  .replaceAll("&gt;", ">")
                                  .replaceAll("&lt;", "<")
                                  .replaceAll("&amp;","&")
                                  .replaceAll("&apos;", "'")
                allMatches += nextNode
              }
            }
            val nextNodes = new Page(allMatches.toList)
            (title, nextNodes)
          })
        })

        val N = originGraph.count
        var graph = originGraph.map(node => {
          node._2.setPageRank(Double.valueOf(1) / Double.valueOf(N))
          (node._1, node._2)
        }).cache()
        
        
        // graph.saveAsTextFile(outputPath)
        // pageRank.saveAsTextFile(outputPath)
        
        val alpha = new Double(0.85)
        var error = new Double(1)
        val one = new Double(1)
        var numOfIteration = 0
        //while (error.compareTo(0.001) >= 0) {
          numOfIteration = numOfIteration + 1
          val zeroDegree = graph.filter(node => {
            node._2.getNumOfEdges() == 0
          }).map(node => {
            node._2.getPageRank()
          }).reduce(_+_) * alpha / Double.valueOf(N) + (one - alpha) * (one / Double.valueOf(N))
         
          val sumOfConstant = sc.broadcast(zeroDegree) 
          println("[ZERODEGREE] " + sumOfConstant.toString())

          graph = graph.flatMap(node => {
            val edges: List[String] = node._2.getEdges()
            val numOfEdges  = Double.valueOf(node._2.getNumOfEdges())
            val pageRank = node._2.getPageRank()
            val nextPageRank = alpha * pageRank / numOfEdges
            edges.map(nextNode => {
              val nextNodePageRank = new Page(edges)
              nextNodePageRank.setPageRank(nextPageRank)
              (nextNode, nextNodePageRank)
            })
          })
          //zeroDegree.saveAsTextFile(outputPath)
        //} 
        
        sc.stop
    }
}

class Page(lk: List[String]) {
  val link: List[String] = lk
  var pageRank: Double = 0.0

  override def toString(): String = {
    link.toString() + "\t" + String.valueOf(pageRank)
  }

  def setPageRank(pg: Double): Unit = {
    pageRank = pg
  }

  def getPageRank(): Double = pageRank

  def getNumOfEdges(): Int = link.length
  
  def getEdges(): List[String] = link
}
        /*
        val counts = lines.flatMap (line => {
            val words = line.split("[^A-Za-z]+")
            words.filter(_.length() != 0).map(word => (word.toLowerCase(), 1))
        }).reduceByKey(_ + _)
        
        val result = counts.sortBy {
          count => (-count._2, count._1)
        };
        
        result.saveAsTextFile(outputPath);
        */
