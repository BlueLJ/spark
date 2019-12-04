package com.blueT.spark.hive

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 2.0对hive支持的初始化  SparkSession
  */
object SparkHiveSession {

  case class Person(name:String,age:Long)
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    var masterUrl = "local[2]"
    var dataPath = "data/"

//    sys.env.get("SPARK_AUDIT_MASTER")

    if (args.length > 0) {
      masterUrl = args(0)
    } else if (args.length > 1) {
      dataPath = args(1)
    }

val conf = new SparkConf().setMaster(masterUrl).setAppName("HiveSpark")
//    val sc = new SparkContext(conf)
    val spark = SparkSession.builder()
      .config(conf)
      .config("spark.sql.warehouse.dir","./hive-warehouse")
      .enableHiveSupport()
      .getOrCreate()
    spark.sql("CREATE TABLE IF NOT EXISTS sr(key INT, value STRING)")
    spark.sql("LOAD DATA LOCAL INPATH 'data/kv1.txt' INTO TABLE src")
    spark.sql("SELECT * FROM src").show()
    spark.sql("SELECT COUNT(*) FROM src").show()

  }
}
