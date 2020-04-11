package com.gs


import java.net.InetAddress

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient

/**
  * 将数据导入到系统：MongoDB 与 ES
  * 使用 Spark SQL 导入数据
  *
  * 解释数据：
  * movies.csv
  * 电影基本信息
  * 用 ^ 隔开
  * 1^
  * Toy Story (1995)^
  * ^
  * 81 minutes^
  * March 20, 2001^
  * 1995^
  * English ^
  * Adventure|Animation|Children|Comedy|Fantasy ^
  * Tom Hanks|Tim Allen|Don Rickles|Jim Varney|Wallace Shawn|John Ratzenberger|Annie Potts|John Morris|Erik von Detten|Laurie Metcalf|R. Lee Ermey|Sarah Freeman|Penn Jillette|Tom Hanks|Tim Allen|Don Rickles|Jim Varney|Wallace Shawn ^
  * John Lasseter
  *
  * 电影ID
  * 电影的名称
  * 电影的描述
  * 电影时长
  * 电影的发行日期
  * 电影拍摄日期
  * 电影的语言
  * 电影的类型
  * 电影的演员
  * 电影的导演
  *
  * ratings.csv
  * 用户对电影的评分数据集
  * 用 , 隔开
  * 1,
  * 31,
  * 2.5,
  * 1260759144
  *
  * 用户ID
  * 电影ID
  * 用户对电影的评分
  * 用户对电影评分的时间
  *
  * tags.csv
  * 用户对电影的标签数据集
  * 15,339,sandra 'boring' bullock,1138537770
  *
  * 用户ID
  * 电影ID
  * 标签内容
  * 时间
  */
object DataLoader {
  //MongoDB 中的表 Collection
  // Moive在MongoDB中的Collection名称【表】
  val MOVIES_COLLECTION_NAME = "Movie"
  // Rating在MongoDB中的Collection名称【表】
  val RATINGS_COLLECTION_NAME = "Rating"

  // Tag在MongoDB中的Collection名称【表】
  val TAGS_COLLECTION_NAME = "Tag"

  //ES TYPE 名称
  val ES_TAG_TYPE_NAME = "Movie"

  val ES_HOST_PORT_REGEX = "(.+):(\\d+)".r


  def main(args: Array[String]): Unit = {
    val DATAFILE_MOVIES = "E:\\BaiduNetdiskDownload\\reco_data\\reco_data\\small\\movies.csv"

    val DATAFILE_RATINGS = "E:\\BaiduNetdiskDownload\\reco_data\\reco_data\\small\\ratings.csv"

    val DATAFILE_TAGS = "E:\\BaiduNetdiskDownload\\reco_data\\reco_data\\small\\tags.csv"
    //创建全局配置
    val params = scala.collection.mutable.Map[String,Any]()
    params += "spark.core" -> "local[2]"
    params += "mongo.uri" -> "mongodb://192.168.207.123:27017/recom"
    params += "monogo.db" -> "recom"
    params += "es.httpHosts" -> "192.168.207.123:9200"
    params += "es.transportHosts" -> "192.168.207.123:9300"
    params += "es.index" -> "recom"
    params += "es.cluster.name" -> "my-application"
    //定义MongoDB配置对象
    implicit val mongoConfig = new MongoConfig(params("mongo.uri")
      .asInstanceOf[String],params("monogo.db").asInstanceOf[String])
    //定义ElasticSearch配置对象
    implicit val eSConf = new ESConfig(params("es.httpHosts")
    .asInstanceOf[String],params("es.transportHosts").asInstanceOf[String],
      params("es.index").asInstanceOf[String],params("es.cluster.name")
    .asInstanceOf[String])
    //声明Spark环境
    val config = new SparkConf().setAppName("DataLoader")
      .setMaster(params("spark.core").asInstanceOf[String])
    val spark = SparkSession.builder().config(config).getOrCreate()
    //加载数据集： Movies  Rating  Tag
    val movieRDD = spark.sparkContext.textFile(DATAFILE_MOVIES)
    val ratingRDD = spark.sparkContext.textFile(DATAFILE_RATINGS)
    val tagRDD = spark.sparkContext.textFile(DATAFILE_TAGS)
    import spark.implicits._
    //将RDD转换成DataFrame
    val movieDF = movieRDD.map(line => {
      val x = line.split("\\^")
      Movie(x(0).trim.toInt,x(1).trim,x(2).trim,x(3).trim,
        x(4).trim,x(5).trim,x(6).trim,x(7).trim,x(8).trim,x(9).trim)
    }).toDF()
    val ratingDF = ratingRDD.map(line =>{
      val x = line.split(",")
      Rating(x(0).trim.toInt,x(1).trim.toInt,x(2).trim.toDouble,x(3).trim.toInt)
    }).toDF()
    val tagDF = tagRDD.map(line => {
      val x = line.split(",")
      Tag(x(0).trim.toInt,x(1).trim.toInt,x(2).trim,x(3).toInt)
    }).toDF()
    //将数据保存到MongoDB
    storeDataInMongo(movieDF,ratingDF,tagDF)
    //缓存
    movieDF.cache()
    tagDF.cache()
    //引入内置函数库
    import org.apache.spark.sql.functions._
    //将 tagDF 对 movieid 做聚合操作，将tag拼接
    val tagCollectDF = tagDF.groupBy($"mid").agg(concat_ws("|"),
      collect_set($"tag").as("tags"))
    //将 tags 合并到movie表，产生新的movie数据集
    val esMovieDF = movieDF.join(tagCollectDF,Seq("mid","mid"),
      joinType = "left").select("mid",
      "name","descri","timelong","issue","shoot",
      "language","genres","actors","directors","tags")
    //将数据保存到ES
    storeDataInES(esMovieDF)
    //去除缓存
    movieDF.unpersist()
    tagDF.unpersist()

    spark.close()
  }
  def storeDataInMongo(movieDF: DataFrame, ratingDF: DataFrame, tagDF: DataFrame)(implicit mongoConfig: MongoConfig): Unit ={
    //创建到MongoDB的链接
    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))
    mongoClient(mongoConfig.db)(MOVIES_COLLECTION_NAME).dropCollection()
    mongoClient(mongoConfig.db)(RATINGS_COLLECTION_NAME).dropCollection()
    mongoClient(mongoConfig.db)(TAGS_COLLECTION_NAME).dropCollection()
    //将Movies数据集写入到MongoDB
    movieDF.write
      .option("uri",mongoConfig.uri)
      .option("collection",MOVIES_COLLECTION_NAME)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
    //将Rating数据集写入到MongoDB
    ratingDF.write
      .option("uri",mongoConfig.uri)
      .option("collection",RATINGS_COLLECTION_NAME)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
    //将Tag数据集写入到MongoDB
    tagDF.write
      .option("uri",mongoConfig.uri)
      .option("collection",TAGS_COLLECTION_NAME)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
    //创建索引
    mongoClient(mongoConfig.db)(MOVIES_COLLECTION_NAME)
      .createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)(RATINGS_COLLECTION_NAME)
      .createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)(RATINGS_COLLECTION_NAME)
      .createIndex(MongoDBObject("uid" -> 1))
    mongoClient(mongoConfig.db)(TAGS_COLLECTION_NAME)
      .createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)(TAGS_COLLECTION_NAME)
      .createIndex(MongoDBObject("uid" -> 1))
    //关闭MongoDB连接
    mongoClient.close()
  }
  def storeDataInES(esMovieDF: DataFrame)(implicit esConf: ESConfig): Unit = {
    //需要操作的Index名称
    val indexName = esConf.index
    //连接ES配置
    val settings = Settings.builder().put("cluster.name",esConf.clusterName)
      .build()
    //连接ES客户端
    val esClient = new PreBuiltTransportClient(settings)
    esConf.transportHosts.split(",")
      .foreach{
        case ES_HOST_PORT_REGEX(host:String,port:String) =>
          esClient.addTransportAddress(new InetSocketTransportAddress(
            InetAddress.getByName(host),port.toInt
          ))
      }
    //判断如果Index存在，则删除
    if (esClient.admin().indices().exists(new
        IndicesExistsRequest(indexName)).actionGet().isExists){
      //删除Index
      esClient.admin().indices().delete(new DeleteIndexRequest(indexName))
        .actionGet()
    }
    //创建Index
    esClient.admin().indices().create(new CreateIndexRequest(indexName))
      .actionGet()
    val movieOptions = Map("es.nodes" -> esConf.httpHosts,
      "es.http.timeout" -> "100m", "es.mapping.id" -> "mid")
    val movieTypeName =s"$indexName/$ES_TAG_TYPE_NAME"
    esMovieDF.write
      .options(movieOptions)
      .mode("overwrite")
      .format("org.elasticsearch.spark.sql")
      .save(movieTypeName)
  }
}
