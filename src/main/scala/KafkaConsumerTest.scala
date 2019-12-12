import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
object KafkaConsumerTest {
  private val topic = Set("kafka-IDS")
  private val broker = "106.39.31.27:9092"
  private val group_id = "consumer-test"
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[4]").setAppName("KafkaConsumer")
    val ssc = new StreamingContext(conf, Seconds(1))
    val kafkaParams = Map[String, Object](
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> broker,
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
        ConsumerConfig.GROUP_ID_CONFIG -> group_id,
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean)
    )
    val stream = KafkaUtils.createDirectStream[String, String](
                            ssc,
                            PreferConsistent,
                            Subscribe[String, String](topic, kafkaParams))
    val value: DStream[String] = stream.map(_.value)
    value.foreachRDD( rdd => {
      rdd.foreachPartition(par => {
        par.foreach(println)
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
