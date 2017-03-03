package lk.lab.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object AccessLogAnalyzer {
  
  def main(args : Array[String]) {
    if (args.length < 1) {
      System.err.println("You should provide a parameter that indicates a file path.")
      System.exit(1)
    }
    
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val file = sc.textFile(args(0))
    file.flatMap(line => line.split(" "))
        .map(word => (word, 1))
        .reduceByKey(_+_)
        .collect()
        .foreach(println)
    
    sc.stop()
  }

}
