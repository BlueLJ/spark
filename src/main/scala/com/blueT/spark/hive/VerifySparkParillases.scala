package com.blueT.spark.hive

import java.sql.{Connection, DriverManager}
import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * 1、验证spark读取数据库数据时的并行度和参数设置的影响
  * 2、验证spark读取hdfs数据时的并行度和参数设置的影响
  */
object VerifySparkParillases {

  def getConnection(): Connection = {
    //you should the database'name not the database and table' name
    DriverManager.getConnection("jdbc:mysql://192.168.137.76:3306/fly-label", "root", "mysql")
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    var masterUrl = "local[3]"
    var dataPath = "data/"

    if (args.length > 0) {
      masterUrl = args(0)
    } else if (args.length > 1) {
      dataPath = args(1)
    }

    val conf = new SparkConf().setMaster(masterUrl).setAppName("SparkDataSet")
    val spark = SparkSession.builder()
      .config(conf)
      .config("spark.sql.warehouse.dir", "hdfs://192.168.137.76:9000/spark/hive-warehouse")
      .config("spark.sql.shuffle.partitions", 1)
      .enableHiveSupport()
      .getOrCreate()


    /**
      * spark连接mysql
      * 第一种：
      */
//    val properties = new Properties()
//    properties.setProperty("user", "root")
//    properties.setProperty("password", "mysql")
//    spark.read.jdbc("jdbc:mysql://192.168.137.76:3306/fly-label", "(select * from fly_knows_areass) T", properties).show()
//
//
//    spark.read.jdbc("jdbc:mysql://192.168.137.76:3306/fly-label", "fly_knows_areass", properties).show(10)

    /**
      * 第二种：
      */
    val map = Map[String, String](
      elems = "url" -> "jdbc:mysql://192.168.137.76:3306/fly-label",
      "user" -> "root",
      "password" -> "mysql",
      "driver" -> "com.mysql.jdbc.Driver",
      "dbtable" -> "fly_knows_areass"
    )
    val data: DataFrame = spark.read.format("jdbc").options(map).load()
    data.show(10)

    data.

    /**
      * 第三种：
      */
//    spark.read.format("jdbc")
//      .option("url", "jdbc:mysql://192.168.137.76:3306/fly-label")
//      .option("driver", "com.mysql.jdbc.Driver")
//      .option("user", "root")
//      .option("password", "mysql")
//      .option("dbtable", "fly_knows_areass")
//      .load()
//      .show(10)

    /**
      * 将数据存储到mysql
      */
//        data.write.mode(SaveMode.Append).jdbc("jdbc:mysql://192.168.137.76:3306/fly-label","res",properties)
    spark.stop()

  }
}
