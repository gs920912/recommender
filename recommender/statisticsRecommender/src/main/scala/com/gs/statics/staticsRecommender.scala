package com.gs.statics

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.{Dataset, SparkSession}

object staticsRecommender {
  val RATE_MORE_MOVIES="RateMoreMovies"
  val RATE_MORE_MOVIES_RECENTLY="RateMoreMoviesRecently"
  val AVERAGE_MOVIES_SCORE = "AverageMoviesScore"
  val GENRES_TOP_MOVIES = "GenresTopMovies"
  //统计评分最多电影
  def rateMore(spark:SparkSession)(implicit mongoConfig: MongoConfig): Unit = {
    val rateMoreDF = spark.sql("select mid,count(1) as count from " +
      "ratings group by mid order by count desc")
    rateMoreDF.write
      .option("uri",mongoConfig.uri)
      .option("collection",RATE_MORE_MOVIES)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }
  //统计 近期热门电影
  def rateMoreRecently(spark:SparkSession)(implicit mongoConfig: MongoConfig): Unit = {
    val simpleDateFormat = new SimpleDateFormat("yyyymm")
    spark.udf.register("changeDate",(x:Long) =>
      simpleDateFormat.format(new Date(x * 1000L)).toLong)
    val yearMonthOfRatings = spark.sql("select mid,uid,score," +
      "changeDate(timestamp) as yearmonth from ratings")
    yearMonthOfRatings.createOrReplaceTempView("ymRatings")
    spark.sql("select mid,count(1) as count, yearmonth " +
      "from ymRatings group by yearmonth,mid order by yearmonth desc,count desc")
      .write
      .option("uri",mongoConfig.uri)
      .option("collection",RATE_MORE_MOVIES_RECENTLY)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }

  // 按类别统计 平均分最高的 Top10
  def genresTop10(spark:SparkSession)(movies:Dataset[Movie])
                 (implicit mongoConfig: MongoConfig): Unit  ={
    //定义所有电影类别
    val genres = List("Action","Adventure","Animation","Comedy","Ccrime","Documentary"
      ,"Drama","Family","Fantasy","Foreign","History","Horror","Music","Mystery"
      ,"Romance","Science","Tv","Thriller","War","Western")
    val averageMovieScoreDF = spark.sql("select mid,avg(score) as avg" +
      "from ratings group by mid").cache()
    //统计 类别中 评分最高的10部电影
    val moviesWithScoreDF = movies.join(averageMovieScoreDF,Seq("mid","mid"))
      .select("mid","avg","genres").cache()
    val genresRDD = spark.sparkContext.makeRDD(genres)
    import spark.implicits._
    val genresTopMovies = genresRDD.cartesian(moviesWithScoreDF.rdd).filter{
      case (genres,row) => {
        row.getAs[String]("genres").toLowerCase.contains(
          genres.toLowerCase)
      }
    }.map{
      case (genres,row) => {
        (genres,(row.getAs[Int]("mid"),
          (row.getAs[Double]("avg")))
      }
    }.groupByKey()
      .map{
        case (genres,items) => {
          GenresRecommendation(genres,items.toList.sortWith(_._2 >
            _._2).take(10).map(x=>Recommendation(x._1,x._2)))
        }
      }.toDF()
    genresTopMovies.write
      .option("uri",mongoConfig.uri)
      .option("collection",GENRES_TOP_MOVIES)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
    averageMovieScoreDF.write
      .option("uri",mongoConfig.uri)
      .option("collection",AVERAGE_MOVIES_SCORE)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }
}
