package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.UserInfo
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import org.json4s.native.Serialization
object UserInfoApp {
  def main(args: Array[String]): Unit = {
    //1.创建sparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("UserInfoApp")

    //2.创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf,Seconds(5))

    //3.获取kafka中的数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_USER,ssc)

    //4.将数据转成样例类
    val userInfoDStream: DStream[UserInfo] = kafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        val userInfo: UserInfo = JSON.parseObject(record.value(), classOf[UserInfo])
        userInfo
      })
    })

    //5.将数据写入redis缓存
    userInfoDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(partition=>{
        implicit val formats = org.json4s.DefaultFormats
        val jedis: Jedis = new Jedis("hadoop102",6379)
        //a.遍历分区下每条数据
        partition.foreach(userInfo=>{
          val userInfoJsonStr: String = Serialization.write(userInfo)
          val redisKey: String = "userInfo:"+userInfo.id
          jedis.set(redisKey,userInfoJsonStr)
        })

        jedis.close()
      })
    })
//    kafkaDStream.foreachRDD(rdd=>{
//      rdd.foreachPartition(partition=>{
//        val jedis: Jedis = new Jedis("hadoop102",6379)
//        partition.foreach(record=>{
//          val userInfo: UserInfo = JSON.parseObject(record.value(),classOf[UserInfo])
//          val redisKey: String = "userInfo:"+userInfo.id
//          jedis.set(redisKey,record.value())
//        })
//
//        jedis.close()
//      })
//    })

    userInfoDStream.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
