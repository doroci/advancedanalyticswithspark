package simple

import java.lang.{RuntimeException => MyError}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by lee on 2017. 2. 13..
  */
object RDDException extends App{


  def func_a(): RDD[String] = {
    val conf = new SparkConf().setAppName("Simple RDDException").setMaster("local[4]")
    val sc = new SparkContext(conf)

    try {
      val a = sc.textFile("/asdfasf")
      a.count()
      a.map(x=>x + "hihi")
    } catch {
      case e:Exception => {
        println("test")
        sc.emptyRDD[String]
      }
    }

//    val a = sc.textFile("/asd")
//    a.map(x=> x+ "hi")

//    func_b(a) match {
//      case Success(rdd) => rdd
//      case Failure(exception) => {
//        throw exception
//      }
//    }

  }

//  import org.apache.spark.rdd.RDD
//  def func_b(rdd: RDD[String]): Try[RDD[String]] = {
//    if (!rdd.isEmpty())
//      Success(rdd)
//    else
//      Failure(new RuntimeException(s"error is $rdd"))
//  }

  func_a().count()
}
