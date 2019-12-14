package com.blueT.spark.hive

import java.sql.{Connection, DriverManager}
import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

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
    var masterUrl = "local[2]"
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
      .config("spark.sql.shuffle.partitions", 2)
      .config("spark.default.parallelism", 4)
      .enableHiveSupport()
      .getOrCreate()

    val rdd = spark.sparkContext.parallelize(ArrayBuffer(1, 5, 6, 7, 8, 2, 3, 4, 9, 10), 3)

    rdd.mapPartitionsWithIndex((partid, iter) => {
      var part_map = scala.collection.mutable.Map[String, List[Int]]()
      var part_name = "part_" + partid
      part_map(part_name) = List[Int]()
      while (iter.hasNext) {
        part_map(part_name) :+= iter.next()
      }
      part_map.iterator
    }).collect.foreach(println)

    val sorted: RDD[(Int, Int)] = rdd.map(x => {
      (x, x)
    }).sortByKey()
    sorted.mapPartitionsWithIndex((partid, iter) => {
      var part_map = scala.collection.mutable.Map[String, List[(Int, Int)]]()
      var part_name = "part_" + partid
      part_map(part_name) = List[(Int, Int)]()
      while (iter.hasNext) {
        part_map(part_name) :+= iter.next()
      }
      part_map.iterator
    }).collect.foreach(println)

    val values: RDD[Int] = sorted.values

    values.map(x => {
      print(x + " ")
    }).collect()

//    values.collect().foreach(println)
    /**
      * spark连接mysql
      * 第一种：1、默认分区策略
      */
    val properties = new Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "mysql")
    //    spark.read.jdbc(
    //      "jdbc:mysql://192.168.137.76:3306/fly-label",
    //      "(select * from fly_knows_areass) T",
    //      properties).show()

    /**
      * spark连接mysql
      * 第一种：2、根据Long类型字段分区
      */
    //        spark.read.jdbc(
    //          "jdbc:mysql://192.168.137.76:3306/fly-label",
    //          "fly_knows_areass",
    //          "id",
    //          1906,
    //          2499,
    //          10,
    //          properties).show()

    /**
      * spark连接mysql
      * 第一种：3、根据任意类型字段分区
      */
    //    val arr = ArrayBuffer[Int]()
    //    for(i <- 0 until 100){
    //      arr.append(i)
    //    }
    //    val predicates =arr.map(i=>{s"SHA1(fieldName)%100 = $i"}).toArray
    //
    //    spark.read.jdbc(
    //      "jdbc:mysql://192.168.137.76:3306/fly-label",
    //      "fly_knows_areass",
    //      predicates,
    //      properties).show(10)

    /**
      * 第二种：
      */
    //    val map = Map[String, String](
    //      elems = "url" -> "jdbc:mysql://192.168.137.76:3306/fly-label",
    //      "user" -> "root",
    //      "password" -> "mysql",
    //      "driver" -> "com.mysql.jdbc.Driver",
    //      "dbtable" -> "fly_knows_areass"
    //    )
    //    val data: DataFrame = spark.read.format("jdbc").options(map).load()
    ////    data.show(10)
    //    println(data.toJavaRDD.partitions.size())
    //    println(data.describe())

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
