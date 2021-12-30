package model

import actor.{KafkaConsumerActor, SupervisorActor}
import akka.actor.{ActorSystem, Props}
import akka.stream.alpakka.cassandra.scaladsl.CassandraSession
import repo.ItemsRepo

object KafkaConsumerActor {
  def props(session: CassandraSession): Props = {
    implicit val cassandraSession = session
    Props(new KafkaConsumerActor())
  }
  case object StartConsuming
}

object SupervisorActor {
  def props(actSystem: ActorSystem, session: CassandraSession): Props = {
    implicit val cassandraSession = session
    implicit val system = actSystem
    Props(new SupervisorActor())
  }
  case object StartConsumer
  case object StartWebserver
}

object ItemsRepo {
  def props(session: CassandraSession): Props = {
    implicit val cassandraSession = session
    Props(new ItemsRepo())
  }
  case object FindAllItems
  case class FindItem(itemId: Int)
  case class FindItemByBrand(brand: String)
  case class FindItemByBrandId(brand: String, id: Int)
}