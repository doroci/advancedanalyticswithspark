package ch2

import org.apache.spark.sql.SparkSession

/**
  * Created by lee on 2017. 2. 5..
  */
object StandAloneTest {


  def main(args: Array[String]) {

    //스파크 세션 설정
    val spark = SparkSession.builder
      .appName("StandAlone-test").master("local[4]")
      .getOrCreate
    import spark.implicits._

    // csv파일 읽기
    val preview = spark
      .read
      .csv("/.../ratings.csv")

    preview.show

    // option값 설정
    val parsed = spark.read
      .option("header", "true") // 파일의 첫 줄을 필드 명으로 사용
      .option("nullValue", "?") // 필드 데이터를 변경( "?" => null )
      .option("inferSchema", "true") // 데이터 타입을 추론한다.
      .csv("/.../ratings.csv")
    parsed.show

    parsed.count
//    parsed.cache
    parsed
      .groupBy("movieId")
      .count
      .orderBy($"count".desc)
      .show

    // createOrReplaceTempView(): 사용중인 DataFrame의 하나의 뷰를 생성한다.
    val createView = parsed.createOrReplaceTempView("parks")
    spark.sql("""
  SELECT movieId, COUNT(*) cnt
  FROM parks
  GROUP BY movieId
  ORDER BY cnt DESC
              """).show

    // describe() : numeric columns, including count, mean, stddev, min, and max의 통계를 리턴해준다.
    val summary = parsed.describe()
    summary.show

    summary.write.format("csv").save("/../output")

  }
}
