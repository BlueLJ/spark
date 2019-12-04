package com.blueT.spark.structuredstreaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object ExamplStructuredStreaming {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    var masterUrl = "local[2]"
    var dataPath = "data/"

    sys.env.get("SPARK_AUDIT_MASTER")

    if (args.length > 0) {
      masterUrl = args(0)
    } else if (args.length > 1) {
      dataPath = args(1)
    }

    val sparkConf = new SparkConf().setMaster(masterUrl).setAppName("examplestructuredstreaming")
    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()
    import spark.implicits._

    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host","192.168.137.76")
      .option("port","9999")
      .load()


    val words: Dataset[String] = lines.as[String].flatMap(_.split(" "))

    val wordcount = words.groupBy("value").count()
    val query: StreamingQuery = wordcount.writeStream.outputMode("complete").format("console").start()
    query.awaitTermination()

  }
}
