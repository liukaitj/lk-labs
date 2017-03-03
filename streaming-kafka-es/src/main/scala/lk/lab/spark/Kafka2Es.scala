package lk.lab.spark

import org.apache.spark.SparkConf  
import org.apache.spark.streaming._  
import org.apache.spark.streaming.kafka._  

object Kafka2Es {
  
  def main(args : Array[String]) {
    val conf = new SparkConf().setAppName("SendJsonFromKafkaToEs")
    val ssc = new StreamingContext(conf, Seconds(3))
    
    val topicMap = Map("test" -> 1)
    val zkQuorum = "bds-hadoop-vm1:2181,bds-hadoop-vm2:2181,bds-hadoop-vm3:2181";
    val group = "test-consumer-group"
    val msgs = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    msgs.print()
    
    ssc.start()
    ssc.awaitTermination()
    
  }
}