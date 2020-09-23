package deltaLakeSDemo

import org.apache.spark.sql.SparkSession

object DeltaLakaDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[4]")
      .appName("DeltaLakaDemo")
      .getOrCreate()

//    val data = spark.range(100, 150)
//    data.write.format("delta").mode("overwrite").save("/usr/local/tmp/delta-table")

//    val df = spark.read.format("delta").load("/usr/local/tmp/delta-table")
//    df.show()

    // Time Travel 通过版本号获取历史版本
//    val ttDf = spark.read.format("delta").option("versionAsOf", 0).load("/usr/local/tmp/delta-table")
//    ttDf.show()

    import io.delta.tables._
    import org.apache.spark.sql.functions._
    val deltaTable = DeltaTable.forPath("/usr/local/tmp/delta-table")
    // update api
//    deltaTable.update(condition = expr("id % 2 = 0"), set = Map("id" -> expr("id + 100") ))

    // delete api
//    deltaTable.delete(condition =  expr("id % 2 != 0"))

    // merge api
//    val newData = spark.range(0, 20).toDF()
//    deltaTable
//        .as("oldData")
//        .merge(newData.as("newData"), "oldData.id = newData.id")
//        .whenMatched
//        .update(
//          Map("id" -> col("newData.id") )
//        )
//        .whenNotMatched
//        .insert(
//          Map("id" -> col("newData.id"))
//        )
//        .execute()

    deltaTable.toDF.show()

  }

}
