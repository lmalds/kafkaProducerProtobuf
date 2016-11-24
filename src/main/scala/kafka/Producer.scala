package kafka

import java.util.Properties

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}


object Producer {
  private val BROKER_LIST = "flink:9092,data0:9092,mf:9092"
  private val props = new Properties()

  props.put("metadata.broker.list", this.BROKER_LIST)
  props.put("serializer.class", "kafka.serializer.DefaultEncoder")
  props.put("producer.type", "async")

  val TOPIC_NAME = "transaction"

  private val config = new ProducerConfig(this.props)
  private val producer = new Producer[Array[Byte], Array[Byte]](this.config)


  def produce(k : Array[Byte], v : Array[Byte]) : Unit = {
    val message = new KeyedMessage[Array[Byte], Array[Byte]](TOPIC_NAME, k, v)
    producer.send(message)
    //println(message)
  }
}
