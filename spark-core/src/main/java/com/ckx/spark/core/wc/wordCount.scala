package com.ckx.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object wordCount {
  def main(args: Array[String]): Unit = {

    // aplicatoin
    // spark 框架
    // TODO 建立和spark框架的连接
    val spareConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(spareConf)

    // TODO 执行业务操作

    val lines: RDD[String] = sc.textFile("data")

    val words: RDD[String] = lines.flatMap(_.split(" "))

    val group: RDD[(String, Iterable[String])] = words.groupBy(word => word)
    val res: RDD[(String, Int)] = group.map {
      case (word, list) => {
        (word, list.size)
      }
    }
    val array: Array[(String, Int)] = res.collect()
    array.foreach(println)
    // TODO 关闭连接
    sc.stop()
  }
}
