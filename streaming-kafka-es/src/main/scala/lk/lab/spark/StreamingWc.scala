package lk.lab.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingWc {
  
  def main(args : Array[String]) {
    val sc = new SparkContext("local[2]", "NetworkWordCount")
    val ssc = new StreamingContext(sc, Seconds(3))
    
    val lines = ssc.socketTextStream("172.16.1.198", 9999)
    val words = lines.flatMap(line => line.split(" "))
    val pairs = words.map(word => (word, 1))
    val wc = pairs.reduceByKey((x, y) => x + y)
    
    wc.print()
    
    ssc.start()
    ssc.awaitTermination()
  }
}