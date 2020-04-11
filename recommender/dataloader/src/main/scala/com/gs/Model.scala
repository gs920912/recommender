package com.gs

/**
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
  */
case class Movie(val mid:Int,val name:String,val descri:String,val timelong:String
                 ,val issue:String,val shoot:String,val language:String,
                 val genres:String,val actors:String , val directors:String)

/**
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
  */
case class Rating(val uid:Int,val mid:Int,val score: Double,val timestamp:Int)

/**
  * tags.csv
  * 用户对电影的标签数据集
  * 15,339,sandra 'boring' bullock,1138537770
  *
  * 用户ID
  * 电影ID
  * 标签内容
  * 时间
  */
case class Tag(val uid:Int,val mid:Int,val tag:String,val timestamp:Int)

/**
  * MongoDB 配置对象
  */
case class MongoConfig(val uri:String,val db:String)

/**
  * ElasticSearch 配置对象
  */
case class ESConfig(val httpHosts:String,val transportHosts:String,val index:String,val clusterName:String)


