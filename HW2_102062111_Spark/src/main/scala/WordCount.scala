/* WordCount.scala */
import org.apache.spark._
import org.apache.hadoop.fs._
import java.util.regex._
import java.util._

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
        val ret = lines.flatMap(line =>{
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
            val allMatches = new ArrayList[String]() 
            while (mNode.find()) {
              val patterns = mNode.group().split("[\\[\\]\\|#]+")
              if (patterns.length > 1) {
                val nextNode = patterns(1).replaceAll("&quot;", "\"")
                                  .replaceAll("&gt;", ">")
                                  .replaceAll("&lt;", "<")
                                  .replaceAll("&amp;","&")
                                  .replaceAll("&apos;", "'")
                allMatches.add(nextNode)
              }
            }
            (title, String.join("\t", allMatches))
          })
        })
        //ret.collect().foreach(println)
        ret.saveAsTextFile(outputPath)
        
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
        sc.stop
    }
}
