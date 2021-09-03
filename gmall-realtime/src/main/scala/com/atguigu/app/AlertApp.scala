package com.atguigu.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{CouponAlertInfo, EventLog}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.util.control.Breaks._
object AlertApp {
  def main(args: Array[String]): Unit = {
    //1.创建sparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("AlertApp").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    //3.获取kafka数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_EVENT, ssc)

    //4.将Json转为样例类，并补全字段,返回KV格式的数据
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
    val midToEventLogDStream = kafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        //将数据转为样例类
        val eventLog: EventLog = JSON.parseObject(record.value(), classOf[EventLog])
        //补全字段
        val times: String = sdf.format(new Date(eventLog.ts))

        eventLog.logDate = times.split(" ")(0)
        eventLog.logHour = times.split(" ")(1)

        (eventLog.mid, eventLog)
      })
    })

    //5.开窗（5分钟的窗口）
    val midToLogWindowDStream = midToEventLogDStream.window(Minutes(5))

    //6.将相同mid的数据聚和到一块
    val midToIterLogDStream: DStream[(String, Iterable[EventLog])] = midToLogWindowDStream.groupByKey()

    //7.根据条件筛选数据 没有浏览商品->领优惠券->符合行为的用户是否大于等于三
    val boolToCouponAlertInfoDStream: DStream[(Boolean, CouponAlertInfo)] = midToIterLogDStream.mapPartitions(partition => {
      partition.map { case (mid, iter) =>
        //创建set集合用来存放用户id
        val uids: util.HashSet[String] = new util.HashSet[String]()
        //创建Set集合用来存放领优惠券所涉及的商品id
        val itemIds: util.HashSet[String] = new util.HashSet[String]()
        //创建list集合用来存放用户涉及到的事件
        val events: util.ArrayList[String] = new util.ArrayList[String]()

        //定义标志位，用来判断是否有浏览商品行为
        var bool: Boolean = true

        //遍历迭代器中的数据
        breakable {
          for (elem <- iter) {
            //将用户行为存放到集合中
            events.add(elem.evid)
            //判断用户是否有浏览商品行为
            if ("clickItem".equals(elem.evid)) {
              //有浏览商品行为
              bool = false

              //跳出循环
              break()
            } else if ("coupon".equals(elem.evid)) {
              //领优惠券行为
              //将用户id存放set集合
              uids.add(elem.uid)
              //将涉及的商品id存放set集合
              itemIds.add(elem.itemid)

            }
          }
        }
        //生成疑似预警日志
        (uids.size() >= 3 && bool, CouponAlertInfo(mid, uids, itemIds, events, System.currentTimeMillis()))

      }
    })

    //8.生成预警日志
    val couponAlertInfoDStream: DStream[CouponAlertInfo] = boolToCouponAlertInfoDStream.filter(_._1).map(_._2)

    couponAlertInfoDStream.print()

    //9.将预警日志写入ES
    couponAlertInfoDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(partition=>{
        val list: List[(String, CouponAlertInfo)] = partition.toList.map(log => {
          (log.mid + log.ts / 1000 / 60, log)
        })
        MyEsUtil.insertBulk(GmallConstants.ES_INDEX_ALERT,list)
      })
    })

    //10.开启任务
    ssc.start()
    ssc.awaitTermination()
  }

}
