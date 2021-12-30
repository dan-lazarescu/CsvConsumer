import org.slf4j.LoggerFactory
import akka.actor.ActorSystem
import akka.stream.alpakka.cassandra.CassandraSessionSettings
import akka.stream.alpakka.cassandra.scaladsl._
import scala.concurrent.ExecutionContext
import model.SupervisorActor._

/**
TODO: 1. Initiate actor system with dependencies
2. start actor system to consume from kafka
3. init & start web server from a separated service object
**/

trait InitSetup {
  implicit val system = ActorSystem()
  implicit val ec = ExecutionContext.global
  val logger = LoggerFactory.getLogger("TyreConsumer")

  //Cassandra SetUp
  private val sessionSettings: CassandraSessionSettings = CassandraSessionSettings()
  val cassandraSession: CassandraSession =
    CassandraSessionRegistry.get(system).sessionFor(sessionSettings)
}

object TyreConsumer extends App with InitSetup {
  val supervisorActor = system.actorOf(model.SupervisorActor.props(system, cassandraSession), "SupervisorActor")
  supervisorActor ! StartConsumer
  supervisorActor ! StartWebserver

}
