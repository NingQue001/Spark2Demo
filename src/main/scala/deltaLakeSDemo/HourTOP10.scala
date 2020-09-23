package deltaLakeSDemo

import java.text.SimpleDateFormat

import io.delta.tables.DeltaTable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

/**
 * 每小时Top 10
 */

/**
 *
 * @param date   日期, yyyy-MM-dd
 * @param hour   小时, HH
 * @param topic  主题
 * @param rank   排名
 * @param num    点击次数
 */
case class HourTOPDefine(date: String, hour: Int, topic: String, rank: Int, num: Int)

object HourTOP10 {
  def main(args: Array[String]): Unit = {
    val inputPath = "/usr/local/tmp/sogou-delta-lake"
    val outputPath = "/usr/local/tmp/sogou-top10-delta-lake"

    val hourSDF = new SimpleDateFormat("yy-MM-dd HH")

    val spark = SparkSession
      .builder()
      .appName("HourTOP100")
      .master("local[4]")
      .getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._

    val inputDF: DataFrame = spark.read.format("delta").load(inputPath).toDF()

    // 注册视图
    /**
     * 数据结构：
     * +----------+----+------+-----------------+----------------------+----------+---------+--------------------+
     * |      date|hour|minute|           userId|                 topic|resultRank|clickRank|                 url|
     * +----------+----+------+-----------------+----------------------+----------+---------+--------------------+
     * |2020-09-23|   0|     0| 6551182914925117|        [尼康相机报价]|         6|        4|product.it168.com...|
     */
    inputDF.createOrReplaceTempView("t_basic")

    // 执行SQL查询，得到 DataFrame
    val queryDF: DataFrame = spark.sql("select date, hour, topic from t_basic")

    val hourWithTopicRDD: RDD[(String, String)] = queryDF.map(line => {
      val date = line.getString(0)
      val hour = line.getInt(1).toString
      date + " " + hour -> line.getString(2)
    }).rdd

    val groupHourTopic: RDD[(String, Iterable[String])] = hourWithTopicRDD.groupByKey()

    val resultRDD: RDD[(String, List[(String, Int)])] = groupHourTopic.mapValues(it => {
      // 定义一个可变Map
      val hourTopicCountMap = mutable.Map[String, Int]()
      it.foreach(topic => {
        var count = hourTopicCountMap.getOrElse(topic, 0)
        count += 1
        hourTopicCountMap.put(topic, count)
      })

      val hourTopicCountList: List[(String, Int)] = hourTopicCountMap.toList
      val sortHourTopicCountList = hourTopicCountList.sortBy(_._2).reverse
      sortHourTopicCountList.take(10) // 只返回Top 10
    })

    val result: RDD[List[HourTOPDefine]] = resultRDD.map(element => {
      val date = element._1.split(" ")(0)
      val hour = element._1.split(" ")(1)
      var rankCounter = 0 // 排名
      val defines: List[HourTOPDefine] = element._2.map(innerElement => {
        val topic = innerElement._1
        val num = innerElement._2
        rankCounter += 1
        HourTOPDefine(date, hour.toInt, topic, rankCounter, num)
      })
      defines
    })

    val finallyResult: RDD[HourTOPDefine] = result.flatMap(x => x)

    finallyResult.toDF().write.mode("overwrite").format("delta").save(outputPath)

    val deltaTable = DeltaTable.forPath(outputPath)
    deltaTable.toDF.show()

    spark.close()
  }
}
