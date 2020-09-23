package deltaLakeSDemo

import java.text.SimpleDateFormat
import java.util.Date

import io.delta.tables.DeltaTable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

case class OutputDefine(date: String, hour: Int, minute: Int
                       , userId: String, topic: String, resultRank: Int
                       , clickRank: Int, url: String)

object SearchTransform {
  def main(args: Array[String]): Unit = {
    val originFilePath = "/usr/local/tmp/search2.txt"
    val outputPath = "/usr/local/tmp/sogou-delta-lake"

    val standardSDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dateSDF = new SimpleDateFormat("yyyy-MM-dd")
    val hourSDF = new SimpleDateFormat("HH")
    val minuteSDF = new SimpleDateFormat("mm")
    val today = dateSDF.format(new Date())

    val spark = SparkSession.builder().master("local[4]").appName("OriginTransform").getOrCreate()
    val sc = spark.sparkContext

    // 引入隐性转换
    import spark.implicits._

    val lineRDD = sc.textFile(originFilePath)
    val resultRDD: RDD[OutputDefine] = lineRDD.map(line => {
      val valueList: Array[String] = line.split("\t")
      val ts = standardSDF.parse(today + " " + valueList(0)).getTime
      val hour = hourSDF.format(new Date(ts)).toInt
      val minute = minuteSDF.format(new Date(ts)).toInt

      val userId = valueList(1)
      val topic = valueList(2)
      val rankList = valueList(3).split("\\s+")
      val resultRank = rankList(0).toInt
      val clickRank = rankList(1).toInt
      val url = valueList(4)

      OutputDefine(today, hour, minute, userId, topic, resultRank, clickRank, url)
    })

    resultRDD.toDF().write.mode("overwrite").format("delta").save(outputPath)

    val deltaTable = DeltaTable.forPath(outputPath)
    deltaTable.toDF.show()

    spark.close()
  }
}
