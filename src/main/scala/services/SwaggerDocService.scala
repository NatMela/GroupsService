package services

import controller.GroupsController
import akka.http.scaladsl.server.Route
import com.github.swagger.akka.SwaggerHttpService
import com.github.swagger.akka.model.Info
import io.swagger.models.ExternalDocs

object SwaggerDocService extends SwaggerHttpService {
  override val apiClasses = Set(classOf[GroupsController])
  override val host = "localhost:8083"
  override val info = Info(version = "1.0")
  override val externalDocs = Some(new ExternalDocs().description("Core Docs").url("http://acme.com/docs"))

  override val unwantedDefinitions = Seq("Function1", "Function1RequestContextFutureRouteResult")


  override def routes: Route = super.routes ~ get {
    pathPrefix("") {
      pathEndOrSingleSlash {
        getFromResource("swagger-ui/index.html")
      }
    } ~
      getFromResourceDirectory("swagger-ui")
  }
}
