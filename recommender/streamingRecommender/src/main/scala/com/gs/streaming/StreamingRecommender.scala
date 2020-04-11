package com.gs.streaming

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object ConnHelper extends Serializable{
  lazy val jedis = new Jedis("192.168.207.123")
  lazy val mongoClient = MongoClient(
    MongoClientURI("mongodb://192.168.207.123:27017/recom"))
}
case class MongoConfig(uri:String,db:String)
//推荐
case class Recommendation(rid:Int,r:Double)
//电影的相似度
case class MovieRecs(mid:Int,recs:Seq[Recommendation])

/**
  * redis
  * lpush uid:1 1129:2.0 1172:4.0 1263:2.0 1287:2.0 1293:2.0 1339:3.5 1343:2.0 1371:2.5
  *
  * lrange uid:1 0 20
  *
  * kafka
1|20|5.0|1564412038
1|12|5.0|1564412037
1|10|5.0|1564412036
1|4|5.0|1564412035
1|6|5.0|1564412034
1|2|5.0|1564412033

  ./bin/kafka-console-producer.sh --broker-list 192.168.109.141:9092 --topic recom
  */
object StreamingRecommender {
  val MAX_USER_RATINGS_NUM = 20
  val MAX_SIM_MOVIES_NUM = 20
  val MONGODB_MOVIE_RECS_COLLECTION = "MovieRecs"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_STREAM_RECS_COLLECTION="StreamRecs"




  def main(args: Array[String]): Unit = {

    val config = Map(
      "spark.core" -> "local[2]",
      "kafka.topic" -> "recom",
      "mongo.uri" -> "mongodb://192.168.207.123:27017/recom",
      "monogo.db" -> "recom"
    )
    //创建 Spark 运行环境
    val sparkConf = new SparkConf().setAppName("StreamingRecommender")
      .setMaster(config("spark.core")).set("spark.executor.memory","4g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc,Seconds(2))
    import spark.implicits._
    implicit val mongoClient = MongoConfig(config("mongo.uri"),
      config("mongo.db"))
    //制作共享变量
    val simMoviesMatrix = spark.read
      .option("uri",mongoClient.uri)
      .option("collection",MONGODB_MOVIE_RECS_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRecs]
      .rdd
      .map{
        recs => (recs.mid,recs.recs.map(x => (x.rid,x.r)).toMap)
      }.collectAsMap()
    //保存所有电影即和它相似的电影列表
    val simMoviesMatrixBroadCast = sc.broadcast(simMoviesMatrix)
    val abc = sc.makeRDD(1 to 2)
    abc.map(x => simMoviesMatrixBroadCast.value.get(1)).count
    val kafkaPara = Map(
      "bootstrap.servers" -> "192.168.207.123:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "recomgroup"
    )
    //连接kafka
    //https://blog.csdn.net/Dax1n/article/details/61917718
    val kafkaStream = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](Array(config
      ("kafka.topic")),kafkaPara))
    //接收评分流    UID|MID|SCORE|TIMESTAMP
    val reatingStream = kafkaStream.map{
      case msg =>
        val attr = msg.value().split("\\|")
        (attr(0).toInt,attr(1).toInt,attr(2).toDouble,attr(3).toInt)
    }
    reatingStream.foreachRDD{
      rdd =>
        rdd.map{
          case (uid,mid,score,timestamp) =>
            println("get data from kafka")
          //获取当前最近的M次评分  redis
          val userRecentlyRatings = getUserRecentlyRating(MAX_USER_RATINGS_NUM,uid,
            ConnHelper.jedis)
          //获取电影P最相似的K个电影   广播变量
          val simMovies = getTioSimMovies(MAX_SIM_MOVIES_NUM,mid,uid,
            simMoviesMatrixBroadCast.value)
          //计算待选电影的推荐优先级
          val streamRecs = computeMovieScores(simMoviesMatrixBroadCast.value,
            userRecentlyRatings,simMovies)

          //将数据保存到MongoDB中
          saveRecsToMongoDB(uid,streamRecs)
        }.count()
    }
    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 将数据保存到 MongoDB中
    * @param uid
    * @param streamRecs  流式计算推荐结果
    * @param mongoConfig
    */
  def saveRecsToMongoDB(uid: Int, streamRecs: Array[(Int, Double)])
                       (implicit mongoConfig: MongoConfig): Unit = {
    val streamRecsCollect = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION)

    streamRecsCollect.findAndRemove(MongoDBObject("uid" -> uid))

    streamRecsCollect.insert(MongoDBObject("uid" -> uid,"recs" -> streamRecs
      .map(x => x._1 + ":" + x._2).mkString("|")))

    println("save to mongoDB")
  }


  /**
    * 计算待选电影的推荐分数
    *
    * @param simMovies            电影相似度矩阵
    * @param userRecentlyRatings  用户最近k次评分
    * @param topSimMovies         当前电影最详细的K的电影
    * @return
    */
  def computeMovieScores(simMovies: scala.collection.Map[Int,
                          scala.collection.immutable.Map[Int, Double]],
                         userRecentlyRatings: Array[(Int, Double)],
                         topSimMovies: Array[Int]): Array[(Int,Double)] = {

    //保存每一个待选电影和最近评分的每一个电影的权重得分
    val score = ArrayBuffer[(Int,Double)]()
    //保存每一个电影的增强因子数
    val increMap = mutable.HashMap[Int,Int]()
    //保存每一个电影的减弱因子数
    val decreMap = mutable.HashMap[Int,Int]()

    for (topSimMovie <- topSimMovies;userRecentlyRating <- userRecentlyRatings){
      val simScore = getMoviesSimScore(simMovies,userRecentlyRating._1,topSimMovie)
      if (simScore > 0.6){
        score += ((topSimMovie,simScore * userRecentlyRating._2))

        if (userRecentlyRating._2 > 3){
          //增强因子起作用
          increMap(topSimMovie) = increMap.getOrElse(topSimMovie,0) + 1
        }else{
          //减弱因子起作用
          decreMap(topSimMovie) = decreMap.getOrElse(topSimMovie,0) + 1
        }
      }
    }
    score.groupBy(_._1).map{
      case (mid,sims) =>
        (mid,sims.map(_._2).sum / sims.length + log(increMap(mid)) - log(decreMap(mid)))
    }.toArray
  }
  def log(i:Int): Double = {
    math.log(i) / math.log(2)
  }
  /**
    * 获取电影之间相似度
    * @param simMovies
    * @param userRatingMovie
    * @param topSimMovie
    * @return
    */
  def getMoviesSimScore(simMovies: scala.collection.Map[Int,
                        scala.collection.immutable.Map[Int, Double]],
                        userRatingMovie: Int, topSimMovie: Int):Double = {

    simMovies.get(topSimMovie) match {
      case Some(sim) => sim.get(userRatingMovie) match {
        case Some(score) => score
        case None => 0.0
      }
      case None => 0.0
    }
  }
  /**
    * 获取当前最近的M次电影评分
    * @param num 评分的个数
    * @param uid 谁的评分
    * @param jedis
    * @return
    */
  def getUserRecentlyRating(num: Int, uid: Int, jedis: Jedis):Array[(Int,Double)] = {
    //redis中保存的数据格式：   uid:1  100:5.0,200:4.9.....
    //从用户队列中取出num个评分
    jedis.lrange("uid；" + uid.toString,0,num).map{
      item =>
        val attr = item.split("\\:")
        (attr(0).trim.toInt,attr(1).trim.toDouble)
    }.toArray
  }

  def getTioSimMovies(num: Int, mid: Int,uid: Int,
                            simMovies:scala.collection.Map[Int,
                              scala.collection.Map[Int,Double]])
                           (implicit mongoConfig: MongoConfig):Array[Int] = {
    //从广播变量的电影相似度矩阵中获取当前电影所有的相似电影
    val allSimMovies = simMovies.get(mid).get.toArray
    //获取用户已经观看过的电影    mongo中获取  类似于redis
    val ratingExist = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION)
      .find(MongoDBObject("uid" -> uid)).toArray.map{
      item =>
        item.get("mid").toString.toInt
    }
    //过滤掉已经评分过的电影，并排序输出
    allSimMovies.filter(x => !ratingExist.contains(x._1)).sortWith(_._2 > _._2)
      .take(num).map(x => x._1)
  }
}
