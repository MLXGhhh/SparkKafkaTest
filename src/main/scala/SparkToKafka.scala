import java.text.SimpleDateFormat
import java.util.{Date, Properties}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}

object SparkToKafka {
  private val topic = "kafka-IDS"
  private val value = "<182>Jan 21 09:58:37 261030KSA2121545(root) 48089616 Threat@IPS: " +
                      "INFO: From 71.29.142.158:57863(xethernet1/1) to 119.254.253.254:16465(xethernet1/1), " +
                      "threat name: MALWARE-BACKDOOR/TROJAN ZeroAccess Outbound udp traffic detected, " +
                      "threat type: Malware, " +
                      "threat subtype: Trojan, " +
                      "App/Protocol: Other-Udp, " +
                      "action: log-only, " +
                      "defender: IPS, " +
                      "signature ID: 840001, " +
                      "profile: tap-a-default-ips, " +
                      "threat severity: Low, " +
                      "policy id:"

  val conf=new SparkConf()
    .setMaster("local[4]")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .setAppName("SparkToKafka")
  val sc=new SparkContext(conf)
  val kafkaProducer:Broadcast[KafkaSink[String,String]]={
    val kafkaProducerConfig={
      val p=new Properties()
      p.setProperty("bootstrap.servers","106.39.31.27:9092")
      p.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
      p.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer")
      p
    }
    sc.broadcast(KafkaSink[String,String](kafkaProducerConfig))
  }
  var flag = true
  val now: Date = new Date()
  var count = 1
  def main(args: Array[String]): Unit = {
    val t1 = new ThreadExample()
    t1.start()
  }
  class ThreadExample extends Thread{
    override def run(){
      while(flag){
        val time: String = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())
        kafkaProducer.value.send(topic,value + time)
        count += 1
        if (now.getTime + 1000 == new Date().getTime){
          flag = false
          println("一秒钟传输数据条数为"+count)
        }
      }
    }
  }
}
