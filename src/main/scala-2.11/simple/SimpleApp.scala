package simple

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by lee on 2017. 1. 26..
  */
object SimpleApp {
  def main(args: Array[String]) {
    val logFile = "/home/ubuntu/spark-2.0.0-bin-hadoop2.7/README.md"
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a"))
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    numAs.saveAsTextFile("/home/ubuntu/spark-2.0.0-bin-hadoop2.7/output")
  }
}


