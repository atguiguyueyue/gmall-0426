package com.atguigu.app

import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{OrderDetail, OrderInfo, SaleDetail}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import redis.clients.jedis.Jedis
import org.json4s.native.Serialization
import collection.JavaConverters._

object SaleDetailApp {
  def main(args: Array[String]): Unit = {
    //1.创建sparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SaleDetailApp")

    //2.创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    //3.获取kafka中的数据
    val kafkaOrderDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER, ssc)

    val kafkaDetailDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER_DETAIL, ssc)

    //4.将两个流的数据分别转为样例类,并且是kv类型，k->指的是join时关联条件 orderId v->数据本身
    val orderInfoDStream = kafkaOrderDStream.mapPartitions(partition => {
      partition.map(record => {
        val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])

        //补全字段  create_time 2021-08-30 19:39:23
        orderInfo.create_date = orderInfo.create_time.split(" ")(0)
        orderInfo.create_hour = orderInfo.create_time.split(" ")(1).split(":")(0)

        (orderInfo.id, orderInfo)
      })
    })

    val orderDetailDStream = kafkaDetailDStream.mapPartitions(partition => {
      partition.map(record => {
        val orderDetail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])
        (orderDetail.order_id, orderDetail)
      })
    })
    //    orderInfoDStream.print()
    //    orderDetailDStream.print()

    //5.双流join，关联订单表和订单明细表数据
    //    val value: DStream[(String, (OrderInfo, OrderDetail))] = orderInfoDStream.join(orderDetailDStream)
    //    value.print()
    val joinDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoDStream.fullOuterJoin(orderDetailDStream)

    //6.通过加缓存的方式来解决数据丢失问题
    joinDStream.mapPartitions(partition => {
      implicit val formats = org.json4s.DefaultFormats
      //创建redis连接
      val jedis: Jedis = new Jedis("hadoop102", 6379)
      //创建list集合用来存放SaleDetail样例类
      val details: util.ArrayList[SaleDetail] = new util.ArrayList[SaleDetail]()

      partition.foreach { case (orderid, (infoOpt, deatilOpt)) =>
        //OrderInfo RedisKey
        val orderInfoRedisKey: String = "orderInfo:" + orderid
        //OrderDetail Rediskey
        val orderDetailRedisKey: String = "orderDetail:"+orderid

        //a.判断orderInfo是否存在
        if (infoOpt.isDefined) {
          //orderInfo存在
          val orderInfo: OrderInfo = infoOpt.get
          //a.2判断orderDetail数据是否存在
          if (deatilOpt.isDefined) {
            //orderDetail存在
            val orderDetail: OrderDetail = deatilOpt.get
            val detail: SaleDetail = new SaleDetail(orderInfo, orderDetail)
            details.add(detail)
          }
          //a.3将orderInfo数据写入redis缓存
          //将样例类转为字符串
          //          JSON.toJSONString(orderInfo)
          val orderInfoJson: String = Serialization.write(orderInfo)

          jedis.set(orderInfoRedisKey, orderInfoJson)
          //设置过期时间
          jedis.expire(orderInfoRedisKey,30)

          //a.4对对方缓存中（orderDetail缓存）查询是否有能够关联上的数据
          //判断对方缓存中是否有能够关联上的rediskey
          if (jedis.exists(orderDetailRedisKey)){
            val orderDetails: util.Set[String] = jedis.smembers(orderDetailRedisKey)
            for (elem <- orderDetails.asScala) {
              //将查询出来的json串转为样例类
              val orderDetail: OrderDetail = JSON.parseObject(elem,classOf[OrderDetail])
              val detail: SaleDetail = new SaleDetail(orderInfo,orderDetail)
              details.add(detail)
            }
          }


        }

      }
      jedis.close()
      partition
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
