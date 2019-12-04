package com.blueT.spark.accumulable

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable.ArrayBuffer

/**
  * 实现自定义Accumulator
  * 功能：把字符串合并成数组的累加器
  */
class MyAccumulable extends AccumulatorV2[String,ArrayBuffer[String]]{ // 输入为String类型，输出为ArrayBuffer[String]
  //设置累加器的输出结果，一个属性
  private var result = ArrayBuffer[String]()

  //用于判断当前累加器的值是否为零值，这里我们自定义size为0，表示累加器是零值
  override def isZero: Boolean = {
    this.result.size == 0
  }

  //copy方法设置为新建本累加器，并把result的值赋给新累加器
  override def copy(): AccumulatorV2[String, ArrayBuffer[String]] = {
    val newAccumulator: MyAccumulable = new MyAccumulable
    newAccumulator.result = this.result
    newAccumulator
  }

  //reset 方法设置为，把result设置为新的ArrayBuffer
  override def reset(): Unit = {
    this.result == new ArrayBuffer[String]()
  }

  //add 用于把传进来的值添加到result中
  override def add(v: String): Unit = {
    this.result += v
  }

  //merge 用于把两个累加器的值合并
  override def merge(other: AccumulatorV2[String, ArrayBuffer[String]]): Unit = {
    this.result.++=:(other.value)
  }

  //value 用于返回result
  override def value: ArrayBuffer[String] = this.result
}

object MyAccumulable{
  def main(args: Array[String]): Unit = {
    //测试自定义累加器

    val myAccumulable = new MyAccumulable
    Logger.getLogger("org").setLevel(Level.ERROR)
    var masterUrl = "local[2]"
    var dataPath = "data/"

    if (args.length > 0) {
      masterUrl = args(0)
    } else if (args.length > 1) {
      dataPath = args(1)
    }

    val sparkConf = new SparkConf().setMaster(masterUrl).setAppName("myaccumulator")
    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()
    val sc = spark.sparkContext
    sc.register(myAccumulable)

    sc.parallelize(Array("a","c","b","d","f","1"),3).foreach(x => myAccumulable.add(x))
    println(myAccumulable)

  }
}