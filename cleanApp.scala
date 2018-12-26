package com.kosun.procedure
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.util.Bytes


import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

import scala.collection.mutable



object cleanApp extends App {


  //如果是从checkpoint中找到offset，则说明之前消费过，checkpoint目录中保留了之前的代码逻辑
  //checkpoint的rdd中保留了消费者组和分区以及offset的信息
  //如果修改了代码逻辑，则必须把checkpoint目录删除，新的代码才会生效，此时会从zk中获取offset的信息
  val streamingContext = StreamingContext.getActiveOrCreate(loadProperties("streaming.checkpoint.path"), createContextFunc())

  streamingContext.start()
  streamingContext.awaitTermination()


  def createContextFunc(): () => _root_.org.apache.spark.streaming.StreamingContext = {
    () => {
      // 创建sparkConf
      val sparkConf = new SparkConf().setAppName("cleanApp").setMaster("local[*]")
      // 配置sparkConf优雅的停止
      sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true")
      // 配置Spark Streaming每秒钟从kafka分区消费的最大速率,每秒读取100条
      sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "100")
      // 指定Spark Streaming的序列化方式为Kryo方式
     // sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // 指定Kryo序列化方式的注册器
     // sparkConf.set("spark.kryo.registrator", "com.kosun.registrator.MyKryoRegistrator")

      // 创建streamingContext
      val interval = loadProperties("streaming.interval")
      val streamingContext = new StreamingContext(sparkConf, Seconds(interval.toLong))
      // 启动checkpoint
      val checkPointPath = loadProperties("streaming.checkpoint.path")
      streamingContext.checkpoint(checkPointPath)

      // 获取kafka配置参数
      val kafka_brokers = loadProperties("kafka.broker.list")
      val kafka_topic = loadProperties("kafka.topic")
      var kafka_topic_set : Set[String] = Set(kafka_topic)
      val kafka_group = loadProperties("kafka.groupId")

      // 创建kafka配置参数Map
      val kafkaParam = Map(
        "bootstrap.servers" -> kafka_brokers,
        "group.id" -> kafka_group
      )

      // 创建kafkaCluster
      val kafkaCluster = new KafkaCluster(kafkaParam)
      // 获取Zookeeper上指定主题分区的offset
      // topicPartitionOffset: HashMap[(TopicAndPartition, offset)]
      val topicPartitionOffset = getOffsetFromZookeeper(kafkaCluster, kafka_group, kafka_topic_set)
      // 创建DirectDStream,高阶API不需要自己管理offset
       //KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](streamingContext, kafkaParam, kafka_topic_set)
      val onlineLogDStream = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder,String](streamingContext, kafkaParam, topicPartitionOffset,(mess: MessageAndMetadata[String, String]) => mess.message())

      // checkpoint原始数据
      onlineLogDStream.checkpoint(Duration(10000))

      // 过滤垃圾数据
   /*   val onlineFilteredDStream = onlineLogDStream.filter{
        case message =>
          var success = true

          success
      }

*/


      // 完成需求统计并写入HBase
      onlineLogDStream.foreachRDD{
        rdd => rdd.foreachPartition{

              println(23)

          items =>
            //val table = getHBaseTabel
            while(items.hasNext){
              val item = items.next()
              println(item)
              // yyyy-MM-dd
             // val dateTime = dateToString(date)
              //val rowKey = dateTime + "_" + startupReportLog.getCity
              //table.incrementColumnValue(Bytes.toBytes(rowKey), Bytes.toBytes("StatisticData"), Bytes.toBytes("userNum"), 1L)

            }
        }
      }

      // 完成需求统计后更新Zookeeper数据


      offsetToZookeeper(onlineLogDStream, kafkaCluster, kafka_group)

      streamingContext
    }
  }

  def offsetToZookeeper(onlineLogDStream: InputDStream[String], kafkaCluster: KafkaCluster, kafka_group: String):Unit = {
    onlineLogDStream.foreachRDD{
      rdd =>
        val offsetsList = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

        for(offsets <- offsetsList){
          val topicAndPartition = TopicAndPartition(offsets.topic, offsets.partition)
          val ack = kafkaCluster.setConsumerOffsets(kafka_group, Map((topicAndPartition, offsets.untilOffset)))
          if(ack.isLeft){
            println(s"Error updating the offset to Kafka cluster: ${ack.left.get}")
          }else{
            println(s"update the offset to Kafka cluster: ${offsets.untilOffset} successfully")
          }
        }
    }
  }

  def getOffsetFromZookeeper(kafkaCluster: KafkaCluster, kafka_group: String, kafka_topic_set: Set[String]) = {
    // 创建Map存储Topic和分区对应的offset
    val topicPartitionOffsetMap = new mutable.HashMap[TopicAndPartition, Long]()
    // 获取传入的Topic的所有分区
    val topicAndPartitions = kafkaCluster.getPartitions(kafka_topic_set)

    // 如果成功获取到Topic所有分区
    if(topicAndPartitions.isRight){
      // 获取分区数据
      val partitions = topicAndPartitions.right.get
      // 获取指定分区的offset
      val offsetInfo = kafkaCluster.getConsumerOffsets(kafka_group, partitions)
      if(offsetInfo.isLeft){
        // 如果没有offset信息则存储0
        for(top <- partitions)
          topicPartitionOffsetMap += (top->0L)
      }else{
        // 如果有offset信息则存储offset
        val offsets = offsetInfo.right.get
        for((top, offset) <- offsets){
         // ([analysis-test1,0],780)
          println((top,offset))
          topicPartitionOffsetMap += (top -> offset)
        }

      }
    }
    topicPartitionOffsetMap.toMap
  }


  def getHBaseTabel = {
    // 创建HBase配置
    val config = HBaseConfiguration.create
    // 设置HBase参数
    config.set("hbase.zookeeper.property.clientPort", loadProperties("hbase.zookeeper.property.clientPort"))
    config.set("hbase.zookeeper.quorum", loadProperties("hbase.zookeeper.quorum"))
    // 创建HBase连接
    val connection = ConnectionFactory.createConnection(config)
    // 获取HBaseTable
    val table = connection.getTable(TableName.valueOf("online_city_click_count"))
    table
  }


  def loadProperties(key:String):String = {
    val properties = new Properties()
    val in = cleanApp.getClass.getClassLoader.getResourceAsStream("config.properties")
    properties.load(in);
    properties.getProperty(key)
  }






}
