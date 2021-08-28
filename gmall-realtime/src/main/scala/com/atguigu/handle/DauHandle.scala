package com.atguigu.handle

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.atguigu.bean.StartUpLog
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object DauHandle {
  /**
    * 进行批次内去重
    * @param filterByRedisDStream
    */
  def filterByGroup(filterByRedisDStream: DStream[StartUpLog]) = {
    //a.将数据转为kv格式 k(mid,logDate) v（startUpLog）
    val midAndLogDateToLogDStream: DStream[((String, String), StartUpLog)] = filterByRedisDStream.map(startUpLog => {
      ((startUpLog.mid, startUpLog.logDate), startUpLog)
    })

    //b.将相同key的数据聚和到一块
    val midAndLogDateToIterLogDStream: DStream[((String, String), Iterable[StartUpLog])] = midAndLogDateToLogDStream.groupByKey()

    //c.对数据按照时间戳进行排序
    val midAndLogDataToListLogDStream: DStream[((String, String), List[StartUpLog])] = midAndLogDateToIterLogDStream.mapValues(iter => {
      iter.toList.sortWith(_.ts < _.ts).take(1)
    })

    //d.将list集合中的数据打散出来
    val value: DStream[StartUpLog] = midAndLogDataToListLogDStream.flatMap(_._2)

    value
  }

  /**
    * 批次间去重
    *
    * @param startUpLogDStream
    */
  def filterByRedis(startUpLogDStream: DStream[StartUpLog], sc: SparkContext) = {
    /* val value: DStream[StartUpLog] = startUpLogDStream.filter(startUpLog => {
       //a.创建redis连接
       val jedis: Jedis = new Jedis("hadoop102", 6379)

       //b.查询redis中保存的mid
       //设置rediskey
       val redisKey: String = "DAU:" + startUpLog.logDate
       val mids: util.Set[String] = jedis.smembers(redisKey)

       //c.拿当前批次的mid与redis中已保存的mid做对比，如果有的话过滤掉，没有的话保留
       //通过contains这个方法可以判断当前数据在集合中是否存在，存在的话返回true，不存在返回false
       val bool: Boolean = mids.contains(startUpLog.mid)

       //关闭连接
       jedis.close()

       //因为是当前数据在redis有的过滤掉，没有的留下来，所以取返回值的一个相反值
       !bool
     })
     value*/

    //方案二：在每个分区下创建连接，以此减少连接个数
    /*val value: DStream[StartUpLog] = startUpLogDStream.mapPartitions(partition => {
      //在分区下创建连接，一个分区创建一个连接
      val jedis: Jedis = new Jedis("hadoop102", 6379)
      val startUpLogs: Iterator[StartUpLog] = partition.filter(startUpLog => {
        //b.查询redis中保存的mid
        //设置rediskey
        val redisKey: String = "DAU:" + startUpLog.logDate
        val mids: util.Set[String] = jedis.smembers(redisKey)

        //c.拿当前批次的mid与redis中已保存的mid做对比，如果有的话过滤掉，没有的话保留
        //通过contains这个方法可以判断当前数据在集合中是否存在，存在的话返回true，不存在返回false
        val bool: Boolean = mids.contains(startUpLog.mid)
        //因为是当前数据在redis有的过滤掉，没有的留下来，所以取返回值的一个相反值
        !bool
      })
      jedis.close()
      startUpLogs
    })
    value*/
    //方案三：每个批次下获取一次连接
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    startUpLogDStream.transform(rdd => {
      //这里面的代码是一个批次执行一次
      //a.创建redis连接(driver)
      val jedis: Jedis = new Jedis("hadoop102", 6379)

      //b.获取redis中的数据(driver)
      val redisKey: String = "DAU:" + sdf.format(new Date(System.currentTimeMillis()))
      val mids: util.Set[String] = jedis.smembers(redisKey)

      //c.将driver端的数据广播到Exetutor端
      val midBC: Broadcast[util.Set[String]] = sc.broadcast(mids)

      val filterMid: RDD[StartUpLog] = rdd.filter(startUpLog => {
        //（executor）
        !midBC.value.contains(startUpLog.mid)
      })
      filterMid
    })
  }

  /**
    * 将去重后的mid保存到redis中（写库）
    *
    * @param startUpLogDStream
    */
  def saveMidToRedis(startUpLogDStream: DStream[StartUpLog]) = {
    startUpLogDStream.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        //a.创建redis连接
        val jedis: Jedis = new Jedis("hadoop102", 6379)
        partition.foreach(startUpLog => {
          //b.将数据（mid）保存至redis
          //设置rediskey
          val redisKey: String = "DAU:" + startUpLog.logDate
          jedis.sadd(redisKey, startUpLog.mid)
        })
        //关闭连接
        jedis.close()
      })
    })
  }

}
