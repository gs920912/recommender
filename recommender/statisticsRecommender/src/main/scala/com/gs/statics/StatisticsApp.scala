package com.gs.statics

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  *
  *
  * 统计的main方法
  *
  * 数据流程：
  * spark读取mongodb中数据，离线统计后，将统计结果写入mongodb
  *
  *
  * 二、离线统计
  **
  *1、目标
  **
  *（1）评分最多电影
  **
  *获取所有历史数据中，评分个数最多的电影集合，统计每个电影评分个数  ->   RateMoreMovies
  **
  *
  *（2）近期热门电影
  **
  *按照月统计，这个月中评分最多的电影我们认为是热门电影，统计每个月中的每个电影的评分数量   ->   RateMoreRecentlyMovie
  **
  *（3）电影的平均分
  **
  *把每个电影，所有用户评分进行平均，计算出每个电影的平均评分    ->    AverageMovies
  **
  *（4）统计每种类别电影Top10
  **
  *将每种类别的电影中，评分最高的10个电影计算出来    ->    GenresTopMovies
  *
  */
object StatisticsApp extends App {
  val RATINGS_COLLECTION_NAME = "Rating"
  val MOVIE_COLLECTION_NAME = "Movie"

  val params = scala.collection.mutable.Map[String,Any]()
  params += "spark.core" -> "local[2]"
  params += "mongo.uri" -> "mongodb://192.168.207.123:27017/recom"
  params += "monogo.db" -> "recom"

  val conf = new SparkConf().setAppName("StatisticsApp").setMaster(
    params("spark.core").asInstanceOf[String])
  val spark = SparkSession.builder().config(conf).getOrCreate()
  //操作mongodb
  implicit val mongoConfig = new MongoConfig(params("mongo.uri")
    .asInstanceOf[String], params("monogo.db").asInstanceOf[String])
  //读取mongodb数据
  import spark.implicits._
  val ratings = spark.read
    .option("uri",mongoConfig.uri)
    .option("collection",RATINGS_COLLECTION_NAME)
    .format("com.mongodb.spark.sql")
    .load()
    .as[Rating].cache()
  val movies = spark.read
    .option("uri",mongoConfig.uri)
    .option("collection",MOVIE_COLLECTION_NAME)
    .format("com.mongodb.spark.sql")
    .load()
    .as[Movie].cache()
  ratings.createOrReplaceTempView("ratings")

  //统计 评分最多电影
  staticsRecommender.rateMore(spark)
  // 近期热门电影
  staticsRecommender.rateMoreRecently(spark)
  //统计每种类别电影Top10
  staticsRecommender.genresTop10(spark)(movies)
  ratings.unpersist()
  movies.unpersist()

  spark.close()

}
