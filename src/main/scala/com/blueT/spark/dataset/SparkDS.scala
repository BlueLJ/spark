package com.blueT.spark.dataset

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.collection.mutable.ArrayBuffer

object SparkDS {

  case class Person(name: String, age: Long)
  case class PersonScore(n: String, score: Long)

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
      .enableHiveSupport()
      .getOrCreate()


    import spark.implicits._
    import org.apache.spark.sql.functions._

    val person: DataFrame = spark.read.json("data/people.json")
    val personScores: DataFrame = spark.read.json("data/peopleScores.json")
    val personDS: Dataset[Person] = person.as[Person]
    val personScoresDS: Dataset[PersonScore] = personScores.as[PersonScore]

    personDS.show()
    personDS.printSchema()

    personDS.map(person => {
      (person.name, person.age + 100L)
    }).show()

    personDS.flatMap(person => person match {
      case Person(name, age) if (name == "Andy") => List((name, age + 70L))
      case Person(name, age) => List((name, age + 30))
    }).show()

    personDS.mapPartitions(persons => {
      val res = ArrayBuffer[(String, Long)]()
      while (persons.hasNext) {
        val p = persons.next()
        res += ((p.name, p.age + 1000L))
      }
      res.iterator
    }).show()

    personDS.dropDuplicates("name").show()
    personDS.distinct().show()

    println("old : " + personDS.rdd.partitions.size)
    println("new1 : " + personDS.repartition(2).rdd.partitions.size)
    println("new2 : " + personDS.repartition(4).coalesce(2).rdd.partitions.size)

    personDS.sort($"age".desc).show()
    personScoresDS.sort($"score".desc).show()

    personDS.join(personScoresDS,$"name" === $"n").show()
    personDS.joinWith(personScoresDS,$"name" === $"n").show()

    val groupPerson = personDS.groupBy($"name",$"age").count()

    groupPerson.show()

    groupPerson.agg(concat($"name",$"age")).show()

    personDS.joinWith(personScoresDS,personDS.col("name") === personScoresDS.col("n")).show()

    personDS.sample(false,0.5).show()

    personDS.randomSplit(Array(10,20)).foreach(dataset => dataset.show())

    personDS.select("name").show()
    spark.stop()

  }
}
