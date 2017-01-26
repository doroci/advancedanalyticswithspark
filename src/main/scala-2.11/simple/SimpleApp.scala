package simple

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by lee on 2017. 1. 26..
  */
object SimpleApp {
  def main(args: Array[String]) {
    val logFile = "/usr/local/spark-2.0.2-bin-hadoop2.7/README.md"
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    sc.stop()
  }
}
