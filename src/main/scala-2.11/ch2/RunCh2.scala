package ch2

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._ // for lit(), first(), etc.



/**
  * Created by lee on 2017. 1. 27..
  */
case class MatchData
(
  id_1: Int,
  id_2: Int,
  cmp_fname_c1: Option[Double],
  cmp_fname_c2: Option[Double],
  cmp_lname_c1: Option[Double],
  cmp_lname_c2: Option[Double],
  cmp_sex: Option[Int],
  cmp_bd: Option[Int],
  cmp_bm: Option[Int],
  cmp_by: Option[Int],
  cmp_plz: Option[Int],
  is_match: Boolean
)

object RunCh2 extends App {
  val spark = SparkSession.builder
    .appName("Intro").master("local[4]")
    .getOrCreate
  import spark.implicits._



  // ex) Users/lee/Documents/sparks/Season2/Advanced_Analytics_With_Spark/linkage/block_1.csv
  val preview = spark.read.csv("/Users/lee/Documents/sparks/Season2/Advanced_Analytics_With_Spark/linkage/block_1.csv")
  preview.show()
  preview.schema.foreach(println)

  val parsed = spark.read
    .option("header", "true") // 파일의 첫 줄을 필드 명으로 사용
    .option("nullValue", "?") // 필드 데이터를 변경( "?" => null )
    .option("inferSchema", "true") // 데이터 타입을 추론한다.
    .csv("/Users/lee/Documents/sparks/Season2/Advanced_Analytics_With_Spark/linkage/block_1.csv")

  parsed.show()
  val schema = parsed.schema
  schema.foreach(println)

  parsed.count()
  parsed.cache()
  parsed.groupBy("is_match").count().orderBy($"count".desc).show()

  parsed.createOrReplaceTempView("parks") // createOrReplaceTempView() : 사용중인 DataFrame의 하나의 뷰를 생성한다.
  spark.sql(
    """
      SELECT is_match, COUNT(*) cnt
      FROM parks
      GROUP BY is_match
      ORDER BY cnt DESC
    """).show()

  val summary = parsed.describe() // describe() : numeric columns, including count, mean, stddev, min, and max의 통계를 리턴해준다.
  // ex)  parsed.describe("id_1","id_2") : 인자값을 넣어 해당 필드만 적용할 수 있다.

  summary.show()

  summary
    .select("summary", "cmp_fname_c1", "cmp_fname_c2")
    .show()

  val matches = parsed.where("is_match = true")
  val misses = parsed.filter($"is_match" === lit(false))
  val matchSummary = matches.describe()
  val missSummary = misses.describe()

  val matchSummaryT = pivotSummary(matchSummary)
  val missSummaryT = pivotSummary(missSummary)
  matchSummaryT.createOrReplaceTempView("match_desc")
  missSummaryT.createOrReplaceTempView("miss_desc")
  spark.sql(
    """
      SELECT a.field, a.count + b.count total, a.mean - b.mean delta
      FROM match_desc a INNER JOIN miss_desc b ON a.field = b.field
      ORDER BY delta DESC, total DESC
    """).show()

  val matchData = parsed.as[
    MatchData] // as() : 새로운 DataSet를 리턴한다.
  val scored = matchData.map(md => {
      (scoreMatchData(md), md.
        is_match)
    }).toDF("score", "is_match")
  crossTabs(scored, 4.0).show()


  def crossTabs(scored: DataFrame, t: Double): DataFrame = {
    scored.
      selectExpr(s"score >= $t as above", "is_match").
      groupBy("above").
      pivot("is_match", Seq("true", "false")). //https://databricks.com/blog/2016/02/09/reshaping-data-with-pivot-in-apache-spark.html 참조
      count()
  }

  case class Score(value: Double) {
    def +(oi: Option[Int]) = {
      Score(value + oi.getOrElse(0))
    }
  }

  def scoreMatchData(md: MatchData): Double = {
    (Score(md.cmp_lname_c1.getOrElse(0.0)) + md.cmp_plz +
      md.cmp_by + md.cmp_bd + md.cmp_bm).value
  }

  def pivotSummary(desc: DataFrame): DataFrame = {
    val lf = longForm(desc)
    lf.groupBy("field").
      pivot("metric", Seq("count", "mean", "stddev", "min", "max")).
      agg(first("value"))
  }

  def longForm(desc: DataFrame): DataFrame = {
    import desc.
    sparkSession.implicits._ // DataFrame으로 변환을 위한 도움을 준다.
    val schema = desc.schema
    desc.flatMap(row => {
      val metric = row.getString(0)
      (1 until row.size).map(i => (metric, schema(i).name, row.getString(i).toDouble))
    })
      .toDF("metric", "field", "value")

  }
}








