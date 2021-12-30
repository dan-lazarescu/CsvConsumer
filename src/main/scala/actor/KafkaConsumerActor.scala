package actor

import org.slf4j.LoggerFactory
import akka.NotUsed
import akka.actor.{Actor, Props}
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.Materializer
import akka.stream.alpakka.cassandra.CassandraWriteSettings
import akka.stream.alpakka.cassandra.scaladsl.{CassandraFlow, CassandraSession}
import akka.stream.scaladsl.{Flow, Sink, Source}
import com.datastax.oss.driver.api.core.cql.{BoundStatement, PreparedStatement}
import io.confluent.kafka.serializers.{AbstractKafkaSchemaSerDeConfig, KafkaAvroDeserializer}
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.{Deserializer, StringDeserializer}
import java.util.UUID.randomUUID
import scala.jdk.CollectionConverters._
import model.Item
import model.KafkaConsumerActor.StartConsuming

class KafkaConsumerActor(implicit val cassandraSession: CassandraSession) extends Actor {
  implicit val mat: Materializer = Materializer(context)
  val logger = LoggerFactory.getLogger(classOf[KafkaConsumerActor])

  override def receive: Receive = {
    case StartConsuming =>
      logger.info("starting flow")
      kafkaSource
        .via(convertFlow)
        .via(insertItemById)
        .via(insertItemByBrandType)
        .via(insertItemByBrandId)
        .runWith(Sink.foreach(println))
    case _ =>
      logger.info("No support for message")
      sender() ! "unsuported message"

  }

  private def genericConsumerSettings(broker: String, schemaRegistry: String, groupId: String): ConsumerSettings[String, GenericRecord] = {
    val kafkaAvroSerDeConfig = Map[String, Any] {
      AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> schemaRegistry
    }
    val kafkaAvroDeserializer = new KafkaAvroDeserializer()
    kafkaAvroDeserializer.configure(kafkaAvroSerDeConfig.asJava, false)

    val deserializer = kafkaAvroDeserializer.asInstanceOf[Deserializer[GenericRecord]]
    ConsumerSettings(context.system, new StringDeserializer, deserializer)
      .withBootstrapServers(broker)
      .withGroupId(groupId)
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  }

  private val consumerSettings = genericConsumerSettings("localhost:9092", "http://localhost:8081", randomUUID().toString())
  private val kafkaSource: Source[ConsumerRecord[String, GenericRecord], Consumer.Control] =
    Consumer.plainSource(consumerSettings, Subscriptions.topics("tyres"))

  private val convertFlow: Flow[ConsumerRecord[String, GenericRecord], Item, NotUsed] =
    Flow[ConsumerRecord[String, GenericRecord]].map { message =>
      // Parsing the record as Item Object
      val genItem = message.value()
      Item(
        genItem.get("itemId").toString.toInt,
        genItem.get("brand").toString,
        genItem.get("itemType").toString,
        genItem.get("description").toString,
        genItem.get("dimensions").toString,
        genItem.get("price").toString.toFloat
      )
    }
  private val statementBinder: (Item, PreparedStatement) => BoundStatement =
    (item, preparedStatement) => preparedStatement.bind(item.itemId, item.itemType, item.brand, item.description, item.dimensions, item.price)

  private val insertItemById: Flow[Item, Item, NotUsed] = {
    CassandraFlow.create(CassandraWriteSettings.defaults,
      s"INSERT INTO user_keyspace.item_by_id(itemId, itemType, brand, description, dimensions, price) VALUES (?, ?, ?, ?, ?, ?)",
      statementBinder)
  }

  private val insertItemByBrandType: Flow[Item, Item, NotUsed] = {
    CassandraFlow.create(CassandraWriteSettings.defaults,
      s"INSERT INTO user_keyspace.item_by_brand_type(itemId, itemType, brand, description, dimensions, price) VALUES (?, ?, ?, ?, ?, ?)",
      statementBinder)
  }

  private val insertItemByBrandId: Flow[Item, Item, NotUsed] = {
    CassandraFlow.create(CassandraWriteSettings.defaults,
      s"INSERT INTO user_keyspace.item_by_brand_id(itemId, itemType, brand, description, dimensions, price) VALUES (?, ?, ?, ?, ?, ?)",
      statementBinder)
  }
}
