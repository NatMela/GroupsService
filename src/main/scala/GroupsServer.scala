import controller.{GroupsController, GroupsDTO, JsonSupport}
import services.{GroupsService, SwaggerDocService}
import controller.{GroupWithUsersDTO, GroupsDTO, GroupsFromPage, GroupsOptionDTO, JsonSupport, LinksDTO, UserGroupsDTO, UserWithGroupsDTO, UsersDTO, UsersFromPage, UsersOptionDTO}


import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.stream.ActorMaterializer
import akka.http.scaladsl.server.RouteConcatenation
import ch.megard.akka.http.cors.scaladsl.CorsDirectives.cors
import com.google.inject.Guice
import com.typesafe.config.ConfigFactory
import config.DiModule
import javax.jms.{Message, MessageListener, Session, TextMessage}
import org.apache.activemq.ActiveMQConnectionFactory
import spray.json.DefaultJsonProtocol

import scala.concurrent.duration._



object GroupsServer extends App with RouteConcatenation {
  implicit def executor: ExecutionContextExecutor = system.dispatcher

  val inject = Guice.createInjector(new DiModule())

  val groupsController = inject.getInstance(classOf[GroupsController])

  implicit val system: ActorSystem = ActorSystem("GroupsServer")
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContext = system.dispatcher

  val routes = cors()(groupsController.groupRoutes ~ SwaggerDocService.routes)

  val host = ConfigFactory.load().getString("serverConf.host")
  val port = ConfigFactory.load().getInt("serverConf.port")

  val serverBinding: Future[Http.ServerBinding] = Http().bindAndHandle(routes, host, port)

  serverBinding.onComplete {
    case Success(bound) =>
      println(s"Server online at http://${bound.localAddress.getHostString}:${bound.localAddress.getPort}/")
    case Failure(e) =>
      Console.err.println(s"Server could not start!")
      e.printStackTrace()
      system.terminate()
  }

  Await.result(system.whenTerminated, Duration.Inf)
}
