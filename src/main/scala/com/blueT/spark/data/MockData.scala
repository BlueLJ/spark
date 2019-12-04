package com.blueT.spark.data

import java.util

import com.alibaba.fastjson.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
  * 模拟生成数据；
  */
object MockData {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf()
      .setMaster("local[1]")
      .setAppName("MockData")
    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()
    val sc = spark.sparkContext
    val strs = new ArrayBuffer[JSONObject]()
    for ( i <- 1 until  100){
      val json = new JSONObject
      json.put("userID",i)

      for (j <- 1 until 10){

      }

    }


  }
}
