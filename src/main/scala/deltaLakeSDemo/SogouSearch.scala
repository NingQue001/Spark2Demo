package deltaLakeSDemo

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SogouSearch {
  def main(args: Array[String]): Unit = {
    val originFilePath = "/Users/mac/Downloads/search2.txt"

    val spark = SparkSession.builder().master("local[4]").appName("OriginTransform").getOrCreate()
    val sc = spark.sparkContext

    //1、启动spark上下文、读取文件
    var orgRdd = sc.textFile(originFilePath)
    println("总行数："+orgRdd.count())

    //2、map操作,遍历处理每一行数据
    var map:RDD[(String,Integer)] = orgRdd.map(line=>{
//      val kv: Array[String] = line.split("\t")
//      println("0: " + kv(0))
//      println("1: " + kv(1))
//      println("2: " + kv(2))
//      println("3: " + kv(3))
//      val intList = kv(3).split("\\s+")
//      println("数字1： " + intList(0).toInt + "数字2： " + intList(1).toInt)
//      println("4: " + kv(4))
      //拿到小时
      var h:String = line.substring(0,2)
              (h,1)
    })

    //3、reduce操作，将上面的 map结果按KEY进行合并、叠加
    var reduce:RDD[(String,Integer)] = map.reduceByKey((x,y)=>{
      x+y
    })

    //打印出按小时排序后的统计结果
    reduce.sortByKey().collect().map(println)
  }
}
