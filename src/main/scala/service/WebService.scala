package service

import org.slf4j.LoggerFactory
import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpHeader, HttpRequest, HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.pattern.ask
import akka.util.Timeout
import model.Item
//import spray.json.DefaultJsonProtocol
import spray.json._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import model.ItemsRepo.{FindAllItems, FindItem, FindItemByBrand, FindItemByBrandId}

import scala.concurrent.Future
import scala.util.{Failure, Success}

trait ItemStoreJsonProtocol extends DefaultJsonProtocol {
  implicit val itemFormat = jsonFormat6(Item)
}

class WebService(dbActor: ActorRef) extends ItemStoreJsonProtocol {
  implicit val actSysytem = ActorSystem()
  implicit val timeout = Timeout(Duration(1000, "millis"))

  val logger = LoggerFactory.getLogger(classOf[WebService])
  private def toHttpEntity(payload: String): HttpEntity.Strict = HttpEntity(ContentTypes.`application/json`, payload)

  private val itemsRoute: Route =
    pathPrefix("api" / "item") {
      get {
        (path(IntNumber) | parameter("id".as[Int])) { itemId =>
          complete(
            (dbActor ? FindItem(itemId))
              .mapTo[Option[Item]]
              .map(_.toJson.prettyPrint)
              .map(toHttpEntity)
          )
        } ~
        (parameter("brand".as[String]) & parameter("brand_id".as[Int])) { (brand, id) =>
          val fut = (dbActor ? FindItemByBrandId(brand, id)).mapTo[(List[Item], String)]
          onComplete(fut) {
            case Success(someItems) =>
              val resultItems = someItems._1
              val nextItemId = someItems._2
              if (resultItems.isEmpty) {
                complete(StatusCodes.NoContent)
              } else {
                respondWithHeader(RawHeader("Next-Item", nextItemId)) {
                  complete(
                    toHttpEntity(resultItems.toJson.prettyPrint)
                  )
                }
              }
            case Failure(ex) =>
              logger.error("Exception encountered:", ex)
              complete(StatusCodes.BadRequest)
          }

        } ~
        parameter("brand".as[String]) { brand =>
          complete(
            (dbActor ? FindItemByBrand(brand))
              .mapTo[List[Item]]
              .map(_.toJson.prettyPrint)
              .map(toHttpEntity)
          )
        } ~
        pathEndOrSingleSlash {
          complete(
            (dbActor ? FindAllItems)
              .mapTo[List[Item]]
              .map(_.toJson.prettyPrint)
              .map(toHttpEntity)
          )
        }
      }
    }

  def startServer(): Future[Http.ServerBinding] = Http().newServerAt("localhost", 8080).bind(itemsRoute)

}
