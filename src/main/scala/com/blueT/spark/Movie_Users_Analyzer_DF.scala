package com.blueT.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object Movie_Users_Analyzer_DF {
  private val logger = Logger.getLogger("org")

  def main(args: Array[String]): Unit = {

    logger.setLevel(Level.INFO)
    var masterUrl = "local[4]"
    var dataPath = "data/"
    if (args.length > 0) {
      masterUrl = args(0)
    } else if (args.length > 1) {
      dataPath = args(1)
    }
    val conf: SparkConf = new SparkConf().setMaster(masterUrl).setAppName("Movie_Users_Analyzer_DF")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    val usersRDD: RDD[String] = sc.textFile(dataPath + "user.dat").cache()
    val moviesRDD: RDD[String] = sc.textFile(dataPath + "movie.dat").cache()
    val ratingsRDD: RDD[String] = sc.textFile(dataPath + "ratings.dat").cache()

    println("功能一：通过DF实现某特定电影钟男性和女性分别有多少人")
    val schemaforusers = StructType(
      "UserID::Age::Gender::Occupation::ZipCode".split("::")
        .map(column => StructField(column, StringType, true))
    )
    val userRDDRows: RDD[Row] = usersRDD.map(_.split("\t")).map(line => Row(line(0).trim, line(1).trim, line(2).trim, line(3).trim, line(4).trim))
    val usersDF: DataFrame = spark.createDataFrame(userRDDRows, schemaforusers)

    val schemaformovies = StructType(
      "MovieID::Title::Genres".split("::")
        .map(column => StructField(column, StringType, true))
    )

    val movieRDDRows: RDD[Row] = moviesRDD.map(_.split("\t")).map(line => Row(line(0).trim, line(1).trim, line(2).trim))
    val movieDF: DataFrame = spark.createDataFrame(movieRDDRows, schemaformovies)

    val schemaforratings = StructType(
      "UserID::MovieID".split("::")
        .map(column => StructField(column, StringType, true))
    ).add("Rating", DoubleType, true)
      .add("TimeStamp", StringType, true)

    val ratingsRDDRows: RDD[Row] = ratingsRDD.map(_.split("\t")).map(line => Row(line(0).trim, line(1).trim, line(2).trim.toDouble, line(3).trim))
    val ratingsDF: DataFrame = spark.createDataFrame(ratingsRDDRows, schemaforratings)

    ratingsDF.filter(s"MovieID = 1193")
      .join(usersDF, "UserID")
      .select("Gender", "Age")
      .groupBy("Gender", "Age")
      .count()
      .show(10)

    /**
      * 功能二：使用另一种语法实现功能一，（sql语句）
      */

    println("功能二：使用另一种语法实现功能一，（sql语句 + GlobalTempView）:")

    ratingsDF.createGlobalTempView("ratings")
    usersDF.createGlobalTempView("users")

    spark.sql("SELECT Gender, Age, count(*) " +
      "FROM global_temp.users as u " +
      "JOIN global_temp.ratings as r " +
      "ON u.UserID=r.UserID " +
      "WHERE MovieID = 1193 GROUP BY Gender, Age")
      .show(10)

    println("功能二：使用另一种语法实现功能一，（sql语句 + LocalTempView）:")

    ratingsDF.createTempView("ratings")
    usersDF.createTempView("users")

    spark.sql("SELECT Gender, Age, count(*) " +
      "FROM users as u " +
      "JOIN ratings as r " +
      "ON u.UserID=r.UserID " +
      "WHERE MovieID = 1193 GROUP BY Gender, Age")
      .show(10)

    /**
      * 功能三：使用DF进行电影流行度分析：所以电影中平均分最高以及观影最多的电影
      */
    println("使用DF进行电影流行度分析：所以电影中平均分最高的电影 (RDD+DF结合)")
    ratingsDF.select("MovieID", "Rating")
      .groupBy("MovieID")
      .avg("Rating")
      .rdd.map(x => (x(1), (x(0), x(1)))).sortBy(_._1.toString.toDouble, false)
      .map(y => y._2)
      .collect().take(10)
      .foreach(println)
    import spark.sqlContext.implicits._

    println("使用DF进行电影流行度分析：所以电影中平均分最高的电影 (DF)")
    ratingsDF.select("MovieID", "Rating")
      .groupBy("MovieID")
      .avg("Rating")
      .orderBy($"avg(Rating)".desc)
      .show(10)

    println("使用DF进行电影流行度分析：所以电影中观影最多高的电影 (RDD+DF)")
    ratingsDF.select("MovieID", "Timestamp").groupBy("MovieID")
      .count().rdd
      .map(x => (x(1).toString.toLong, (x(0), x(1))))
      .sortByKey(false)
      .map(_._2)
      .collect()
      .take(10)
      .foreach(println)

    println("使用DF进行电影流行度分析：所以电影中观影最多高的电影 (DF)")
    ratingsDF.groupBy("MovieID")
      .count().orderBy($"count".desc).show(10)

    /**
      * 功能四：分析最受男性喜爱的电影ToplO 和最受女性喜爱的电影T o pl O
      * 分析：
      * 1、"users.dat": UserID : : Age: :Gender  : :Occupation: :Zip-code
      * 2、单从ratings 中无法计算出最受男性或者女性喜爱的电影Topl O ，因为该RD D 中
      * 没有Gender 信息，如果我们需要使用Gender 信息进行Gender 的分类，此时一定市要聚合，
      * 当然， 我们力求聚合使用的是mapjoin （分布式计算的Killer是数据倾斜， Mapper 端的
      * Join 是一定不会数据倾斜的），这里可否使用mapjoin呢？不可以，因为用户的数据非常多！所以这里要使用正常的Join ，此处的场景
      * 不会数据倾斜，因为用户一般都很均匀地分布（但是系统信息搜集端要注意黑客攻击）
      *
      * 注意：
      * 1、因为要再次使用电影数据的RDD  所以复用了前面Cache 的ratings 数据：
      * 2、在根据性别过滤出数据后，关于TopN 部分的代码，直接复用前面的代码就行了
      * 3、要进行Jo in 的话， 需要key-value;
      * 4、 在进行Jo in 的时候通过take 等方法注意Join 后的数据格式，例如
      * 5、使用数据冗余实现代码复用或者更高效地运行，这是企业级项目的一个非常重要的技巧！
      */

    val male = "M"
    val female = "F"

    val genderRatings: RDD[(String, ((String, String, String), String))] = ratingsRDD.map(_.split("\t")).map(x => (x(0), x(1), x(2)))
      .map(x => (x._1, (x._1, x._2, x._3)))
      .join(usersRDD.map(_.split("\t")).map(x => (x(0), x(1)))
        .cache())
    val genderDF: DataFrame = ratingsDF.join(usersDF, "UserID").cache()
    genderRatings.take(10).foreach(println)
    val maleFilteredRatings = genderRatings.filter(_._2._2.equals(male)).map(_._2._1)
    val maleFilteredRatingDF = genderDF.filter("Gender='M'").select("MovieID", "Rating")

    val femaleFilteredRatings = genderRatings.filter(_._2._2.equals(female)).map(_._2._1)
    val femaleFilteredRatingDF = genderDF.filter("Gender='F'").select("MovieID", "Rating")

    println("纯粹使用RDD 实现所有电影中最受男性喜爱的电影ToplO ： ")
    maleFilteredRatings.map(x => (x._2, (x._3.toDouble, 1)))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .map(x => (x._2._1.toDouble / x._2._2, x._1))
      .sortByKey(false)
      .map(x => (x._2, x._1))
      .take(10).foreach(println)

    println("纯粹使用DF 实现所有电影中最受男性喜爱的电影ToplO ： ")
    maleFilteredRatingDF.groupBy("MovieID").avg("Rating").orderBy($"avg(Rating)".desc).show(10)

    //女性的同理。。。
    /**
      * 最受不同年龄段人员欢迎的电影TopN  （DF）
      * 注意：
      * 1、不同年龄阶段如何界定
      * 2、性能问题：map端join
      */
    ratingsDF.join(usersDF, "UserID").filter("Age='18'").groupBy("MovieID")
      .count().orderBy($"count".desc).printSchema()

    /**
      * orderBy操作需要在Join之后
      */
    println("纯粹通过DataFrame 的方式实现所有电影中QQ 或者微信核心用户最喜爱也影TopN分析")
    ratingsDF.join(usersDF, "UserID").filter("Age='18'")
      .groupBy("MovieID")
      .count()
      .join(movieDF, "MovieID")
      .select("Title", "count")
      .orderBy($"count".desc)
      .show(10)


    sc.stop()
    spark.close()
  }
}
