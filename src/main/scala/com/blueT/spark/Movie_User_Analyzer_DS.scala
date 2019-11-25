package com.blueT.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * 用户观看电影和点评电影 的所有行为数据的采集、过滤、处理和展示
  *
  * 数据采集：直接发送到Kafka，更具备实时性
  * 数据过滤：使用Spark SQL 进行过滤
  * 数据处理：
  * 1、先使用传统的sql去实现一个数据处理的业务逻辑；
  * 2、再次推荐使用DataSet去实现业务功能尤其是统计分析功能；
  * 3、使用RDD最好；
  * 4、使用Parquet；
  */

object Movie_User_Analyzer_DS {

  case class User(UserId: String, Age: String, Gender: String, Occupation: String, ZipCode: String)

  case class Rating(UserID: String, MovieID: String, Rating: Double)

  case class Movie(MovieID: String, Title: String, Genres: String)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    var masterUrl = "local[2]"
    var dataPath = "data/"

    if (args.length > 0) {
      masterUrl = args(0)
    } else if (args.length > 1) {
      dataPath = args(1)
    }

    val sparkConf = new SparkConf().setMaster(masterUrl).setAppName("MovieUserAnalyzerDataSet")
    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()
    val sc = spark.sparkContext

    val usersRDD: RDD[String] = sc.textFile(dataPath + "user.dat").cache()
    val moviesRDD: RDD[String] = sc.textFile(dataPath + "movie.dat").cache()
    val ratingsRDD: RDD[String] = sc.textFile(dataPath + "ratings.dat").cache()

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


    import spark.implicits._
    val usersDS: Dataset[User] = usersDF.as[User]
    val movieDS: Dataset[Movie] = movieDF.as[Movie]
    val ratingDS: Dataset[Rating] = ratingsDF.as[Rating]
    println("功能一：通过DF 实现某特定电影观看者巾男性和女性不问年龄分别有多少人？")
    ratingsDF.filter(s"MovieID=1193")
      .join(usersDF, "UserID")
      .select("Gender", "Age")
      .groupBy("Gender", "Age")
      .count()
      .show(10)


    println("功能一：通过Dataset 实现某特定电影观看者巾男性和女性不问年龄分别有多少人？")
    ratingDS.filter(s"MovieID=1193")
      .join(usersDF, "UserID")
      .select("Gender", "Age")
      .groupBy("Gender", "Age")
      .count()
      .show(10)

    println("功能一：通过global sql实现某特定电影观看者巾男性和女性不问年龄分别有多少人？")
    ratingsDF.createGlobalTempView("ratings")
    usersDF.createGlobalTempView("users")

    spark.sql("SELECT Gender, Age, count(*) FROM global_temp.users as u JOIN global_temp.ratings as r ON u.UserID = r.UserID WHERE MovieID = 1193 group by Gender, Age").show(10)

    println("功能一：通过tmp sql实现某特定电影观看者巾男性和女性不问年龄分别有多少人？")
    ratingsDF.createTempView("ratings")
    usersDF.createTempView("users")

    spark.sql("SELECT Gender, Age, count(*) FROM users as u JOIN ratings as r ON u.UserID = r.UserID WHERE MovieID = 1193 group by Gender, Age").show(10)

    println("纯粹通过Data Set 的方式计算最流行电影（即所有电影中粉丝或者观看人数最多）的电影TopN ：")

    ratingDS.groupBy("MovieID").count()
      .orderBy($"count".desc).show(10)
    println("功能四：分析最受男性喜爱的电影ToplO 和最受女性喜爱的电影ToplO")

    val genderRatingDS = ratingDS.join(usersDS, "UserID").cache()
    val maleRatingsDS = genderRatingDS.filter("Gender = 'M'").select("MovieID", "Rating")
    val famaleRatingsDS = genderRatingDS.filter("Gender = 'F'").select("MovieID", "Rating")
    maleRatingsDS.groupBy("MovieID")
      .avg("Rating")
      .orderBy($"avg(Rating)".desc)
      .show(10)

    famaleRatingsDS.groupBy("MovieID")
      .avg("Rating")
      .orderBy($"avg(Rating)".desc)
      .show(10)

    /**
      * 思考题：如果想让RDD 和DataFrame 计算的TopN 的每次结果都一样，该如何
      * 保证？现在的情况是，例如计算ToplO ，而其同样评分的不止10 个，所以每次都会
      * 从中取出10 个，这就导致大家的结果不一致，这个时候，我们可以使用一个新的
      * 列参与排序2
      * 如果是ROD ，该怎么做呢？这时就要进行二次排序，如果是DataFrame ，该如何做呢？非常简单，我们
      * 只需要在orderBy 函数中增加一个排序维度的字段即可
      */

    println("功能五：段受不同年龄段人员欢迎的电影TopN")
    ratingDS.join(usersDS, "UserID").filter("Age='18'")
      .groupBy("MovieID")
      .count()
      .orderBy($"count".desc).printSchema()

    ratingDS.join(usersDS, "UserID")
      .filter("Age = '25'")
      .groupBy("MovieID")
      .count()
      .join(movieDS, "MovieID")
      .select("Title", "count")
      .sort($"count".desc)
      .limit(10)
      .show()


    sc.stop()
    spark.stop()
  }

}
