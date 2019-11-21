package com.blueT.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.HashSet

object Movie_Users_Analyzer {
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

    val sc: SparkContext = new SparkContext(new SparkConf().setMaster(masterUrl).setAppName("Movie_Users_Analyzer"))
    val usersRDD: RDD[String] = sc.textFile(dataPath + "user.dat")
    val moviesRDD: RDD[String] = sc.textFile(dataPath + "movie.dat")
    val ratingsRDD: RDD[String] = sc.textFile(dataPath + "ratings.dat")

    val userBasic: RDD[(String, (String, String, String))] = usersRDD.map(_.split("\t")).map {
      user => (user(3), (user(0), user(1), user(2)))
    }
    for (elem <- userBasic.collect().take(2)) {
      println("userBasicRDD(职业，(用户id，性别，年龄)):" + elem)
      logger.info("userBasicRDD(职业，(用户id，性别，年龄)):" + elem)
      logger.error("TT:" + elem)
    }
    val targetUsers: RDD[(String, ((String, String, String), String))] = userBasic.map(x => (x._2._1, (x._2, x._1)))

    for (elem <- targetUsers.collect().take(2)) {
      println("targetUsers(用户id,(用户id，性别，年龄),职业名):" + elem)
    }
    val movieInfo: RDD[(String, String, String)] = moviesRDD.map(_.split("\t")).map {
      movie => {
        (movie(0), movie(1), movie(2))
      }
    }
    for (elem <- movieInfo.collect().take(2)) {
      println("movieInfo(电影id，电影名，电影类型):" + elem)
    }
    val targetMovie: RDD[(String, String)] = ratingsRDD.map(_.split("\t"))
      .map(x => (x(0), x(1))).filter(_._2.equals("1193"))
    for (elem <- targetMovie.collect().take(2)) {
      println("targetMovie(用户id，电影id):" + elem)
    }
    println("电影点评系统用户行为分析，统计观看电影ID为1193的电影用户信息：用户的ID 、性别、年龄、职业名")
    val userInfoForMovie: RDD[(String, (String, ((String, String, String), String)))] = targetMovie.join(targetUsers)
    //    println(userInfoForMovie.collect().length)
    for (elem <- userInfoForMovie.collect().take(10)) {
      println("用户id，(电影id,((用户id，性别，年龄),职业名)) ：" + elem)
    }

    /**
      * 11月20日
      * 一、统计所有电影中平均得分最高的电影top10
      * 思路：reduceByKey()算子
      * 1、把数据变成K-V,(movieId作为key，rating作为value)
      * 2、聚合：reduceByKey()算子
      * 3、排序
      * 二、观看人数最多的电影 top10
      * 思路：
      * 1、把数据变成Key-Value ：取ratings元组的第2个元素电影ID 作为Key ，计数l
      * 次作为Value ，格式化成为Key-Value ，即（电影ID, 1 ）。
      * 2、通过reduceByKey 操作实现聚合： 对相同Key 的Va lue 值进行累加。生成
      * Key-Value ，即（电影ID ， 总次数〉。
      * 3、排序，进行Key 和Value 的交换。上一步reduceB yKey 算子执行完毕，然后进
      * 行map 转换操作， 交换Key-Value 值，即将（电影田， 总次数）转换成（总次数，电影ID) '
      * 然后使用sortByKey( false）算子按总次数降序排列。
      * 4、 再次进行Key 和Value 的交换，打印输出。我们使用map 转换函数将（总次数，
      * 电影ID ）进行交换，转换为（电影ID ， 总次数），再通过take(lO） 算子获取所有电影中粉丝
      * 或者观看人数最多的电影ToplO ，打印输出。
      */

    println("所有电影中平均得分最高（口碑最好）的电影：")
    val ratings: RDD[(String, String, String)] = ratingsRDD.map(_.split("\t")).map(x => (x(0), x(1), x(2))).cache()

    ratings.map(x => (x._2, (x._3.toDouble, 1)))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .map(x => (x._2._1.toDouble / x._2._2, x._1))
      .sortByKey(false)
      .take(10)
      .foreach(println)

    println("所有电影中粉丝或者观看人数最多的电影：")
    ratings.map(x => (x._2, 1))
      //      .reduceByKey((x,y) => (x + y))
      .reduceByKey(_ + _)
      .map(x => (x._2, x._1))
      .sortByKey(false)
      .map(x => (x._2, x._1))
      .take(10)
      .foreach(println)


    /**
      * 11月20日：
      * 需求：分析大数据电影点评系统各种类型的最喜爱电影TopN 。
      * 本节分析最受男性喜爱的电影ToplO 和最受女性喜爱的电影ToplO 。
      *
      * 题外话：需要使用join，一般join都力求 map端join，避免数据倾斜，但是此处不考虑
      *
      * 重点：1、需要复用前面的RDD，所以cache().
      * 2、topN代码同样可以复用
      * 3、使用数据冗余来实现代码复用或更高效的运行
      */
    val male = "M"
    val female = "F"

    val users: RDD[(String, String)] = usersRDD
      .map(_.split("\t"))
      .map(x => (x(0), x(2)))

    val genderRatings: RDD[(String, ((String, String, String), String))] = ratings.map(x => (x._1, (x._1, x._2, x._3)))
      .join(users).cache()
    genderRatings.take(2).foreach(println)

    val maleRatings: RDD[(String, String, String)] = genderRatings.filter(x => x._2._2.equals(male)).map(x => x._2._1)
    val femaleRatings: RDD[(String, String, String)] = genderRatings.filter(x => x._2._2.equals(female)).map(x => x._2._1)

    println("所有电影中最受男性喜爱的电影ToplO ：")
    maleRatings.map(x => (x._2, (x._3.toDouble, 1)))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .map(x => (x._2._1.toDouble / x._2._2, x._1))
      .sortByKey(false)
      .map(x => (x._2, x._1))
      .take(10)
      .foreach(println)

    println("所有电影中最受女性喜爱的电影ToplO ：")
    femaleRatings.map(x => (x._2, (x._3.toDouble, 1)))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .map(x => (x._2._1.toDouble / x._2._2, x._1))
      .sortByKey(false)
      .map(x => (x._2, x._1))
      .take(10)
      .foreach(println)
    /**
      * 11月20日：
      * 需求：分析大数据电影点评系统仿QQ 和微信等用户群分析，在本节统计最受不同
      * 年龄段人员欢迎的电影TopN 。
      * 思路：首先计算topN 但是注意：
      * 1、不同年龄阶段如何定界问题：这个问题其实是业务问题。一般情况下， 我们都是在原始数据中直接对要进行分组的年
      * 龄段提前进行数据清洗ETL ，例如，进行数据清洗ETL 后产生以下数据。 小于18岁，18< age < 24等等；
      * 2、性能问题：
      * a、在实现的时候可以使用RDD 的filter 算子，如13 < age < 18 ， 但这样做会导致
      * 运行时进行大量的计算， 因为要进行扫描， 所以非常耗性能， 通过提前进行数据清洗ETL 把
      * 计算发生在Sp ark 的业务逻辑运行前， 用空间换时间， 当然， 这些实现也可以使用Hive ， 因
      * 为Hive 语法支持非常强悍且内置了最多的函数。
      * b、这里要使用mapjoin ， 原因是targetUs ers 数据只有User ID ， 数据量一般不会太大。
      *
      */

    val targetQQUsers: RDD[(String, String)] = usersRDD
      .map(_.split("\t"))
      .map(x => (x(0), x(1)))
      .filter(_._2.equals("18"))

    val targetTaoBaoUsers: RDD[(String, String)] = usersRDD
      .map(_.split("\t"))
      .map(x => (x(0), x(1)))
      .filter(_._2.equals("25"))

    /**
      * spark实现map端join，借助Broadcast，广播小数据量到Executor，直接让
      * 该Executor的所有任务共享一份数据，而不是每次执行Task都要发送一份数据
      * 的复制，降低网络IO和JVM内存消耗
      */

    val targetQQUsersSet = HashSet() ++ targetQQUsers.map(_._1).collect()
    val targetTaoBaoUsersSet = HashSet() ++ targetTaoBaoUsers.map(_._1).collect()
    val targetQQUsersBroadcast = sc.broadcast(targetQQUsersSet)
    val targetTaoBaoUsersBroadcast = sc.broadcast(targetTaoBaoUsersSet)

    val movieId2Name: Map[String, String] = moviesRDD.map(_.split("\t"))
      .map(x => (x(0), x(1))).collect().toMap

    println("所有电影中QQ 用户最喜爱电影TopN 分析：")
    ratingsRDD.map(_.split("\t"))
      .map(x => (x(0), x(1)))
      .filter(x => targetQQUsersBroadcast.value.contains(x._1))
      .map(x => (x._2,1))
      .reduceByKey(_+_)
      .map(x => (x._2,x._1))
      .sortByKey(false)
      .map(x => (x._2,x._1))
      .take(10)
      .map(x => (movieId2Name.getOrElse(x._1,null),x._2))
      .foreach(println)

    println("所有电影中淘宝核心目标用户最喜爱电影TopN10分析：")
    ratingsRDD.map(_.split("\t"))
      .map(x => (x(0), x(1)))
      .filter(x => targetTaoBaoUsersBroadcast.value.contains(x._1))
      .map(x => (x._2,1))
      .reduceByKey(_+_)
      .map(x => (x._2,x._1))
      .sortByKey(false)
      .map(x => (x._2,x._1))
      .take(10)
      .map(x => (movieId2Name.getOrElse(x._1,null),x._2))
      .foreach(println)
    sc.stop()
  }
}
