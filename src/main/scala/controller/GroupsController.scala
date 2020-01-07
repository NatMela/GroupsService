package controller


import akka.NotUsed
import akka.http.scaladsl.common.{EntityStreamingSupport, JsonEntityStreamingSupport}
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.marshalling.Marshal
import services.GroupsService
import akka.http.scaladsl.model.{HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.directives.RouteDirectives.complete
import akka.stream.scaladsl.{Flow, Source}
import akka.util.ByteString
import com.google.inject.{Guice, Inject}
import config.{Db, DiModule}
import dao.{GroupsDAO, UserGroupsDAO}
import io.pileworx.akka.http.rest.hal.{Link, ResourceBuilder}
import io.swagger.annotations._
import javax.ws.rs.{GET, Path}
import org.slf4j.LoggerFactory
import io.pileworx.akka.http.rest.hal.Relations._
import spray.json._

import scala.concurrent.ExecutionContext

case class UsersDTO(id: Option[Int], firstName: String, lastName: String, createdAt: Option[String], isActive: Boolean)

case class UsersOptionDTO(id: Option[Int], firstName: Option[String], lastName: Option[String], createdAt: Option[String], isActive: Option[Boolean])

case class GroupsDTO(id: Option[Int], title: String, createdAt: Option[String], description: String)

case class GroupsOptionDTO(id: Option[Int], title: Option[String], createdAt: Option[String], description: Option[String])

case class UsersFromPage(users: Seq[UsersDTO], totalAmount: Int, pageNumber: Int, pageSize: Int)

case class GroupsFromPage(groups: Seq[GroupsDTO], totalAmount: Int, pageNumber: Int, pageSize: Int)

case class UserGroupsDTO(id: Option[Int], userId: Int, groupId: Int)

case class UserWithGroupsDTO(userInfo: UsersDTO, groups: Seq[GroupsDTO])

case class GroupWithUsersDTO(groupInfo: GroupsDTO, users: Seq[UsersDTO])

case class LinksDTO(ref: String, link: String)

trait JsonSupport extends SprayJsonSupport with DefaultJsonProtocol {
  implicit val userFormat = jsonFormat5(UsersDTO)
  implicit val userOptionFormat = jsonFormat5(UsersOptionDTO)
  implicit val groupsFormat = jsonFormat4(GroupsDTO)
  implicit val groupsOptionFormat = jsonFormat4(GroupsOptionDTO)
  implicit val usersPageFormat = jsonFormat4(UsersFromPage)
  implicit val groupsPageFormat = jsonFormat4(GroupsFromPage)
  implicit val userGroupsFormat = jsonFormat2(UserWithGroupsDTO)
  implicit val groupsUserFormat = jsonFormat2(GroupWithUsersDTO)
  implicit val usersGroupsFormat = jsonFormat3(UserGroupsDTO)
  implicit val linksFormat = jsonFormat2(LinksDTO)
}


trait Links extends JsonSupport {
  def groupLink(rel: String, id: String): (String, Link) = rel -> Link(href = s"/groups/$id")

  def groupsLink(rel: String, pageSize: String, pageNumber: String): (String, Link) = rel -> Link(href = s"/groups?pageSize=$pageSize&pageNumber=$pageNumber")

  def groupsAllLink(rel: String): (String, Link) = rel -> Link(href = s"/groups/all")

  def groupDetailsLink(rel: String, id: String): (String, Link) = rel -> Link(href = s"/groups/$id/details")

  def groupUserLink(rel: String, groupId: String, userId: String): (String, Link) = rel -> Link(href = s"/groups/$groupId/users/$userId")

  def toResourcesFromPage(groups: GroupsFromPage): JsValue = {
    ResourceBuilder(
      withEmbedded = Some(Map(
        "groups" -> groups.groups.map(f => toResource(f))
      )),
      withLinks = Some(Map(
        groupsAllLink(Up),
        groupsLink(Self, groups.pageSize.toString, groups.pageNumber.toString)))
    ).build
  }

  def toResources(groups: Seq[GroupsDTO]): JsValue = {
    ResourceBuilder(
      withEmbedded = Some(Map(
        "groups" -> groups.map(f => toResource(f))
      )),
      withLinks = Some(Map(
        groupsAllLink(Self),
        groupsLink(Up, "1", "1")))
    ).build
  }

  def toResource(group: GroupsDTO): JsValue = {
    ResourceBuilder(
      withData = Some(group.toJson),
      withLinks = Some(Map(
        groupLink(Self, group.id.get.toString),
        groupsAllLink(Up),
        groupDetailsLink(Next, group.id.get.toString)
      ))
    ).build
  }

  def toResourceDetails(group: GroupWithUsersDTO): JsValue = {
    ResourceBuilder(
      withData = Some(group.toJson),
      withLinks = Some(Map(
        groupDetailsLink(Self, group.groupInfo.id.get.toString),
        groupLink(Prev, group.groupInfo.id.get.toString),
        groupsAllLink(Up)
      ))
    ).build
  }

  def getPaths: JsValue = {
     val links: Seq[(String, Link)] =  Seq(
        groupsAllLink("GET"),
        groupsLink("GET", "pageSize", "pageNumber"),
        groupDetailsLink("GET", "{id}"),
        groupLink("GET", "{id}"),
        groupLink("PUT", "{id}"),
        groupLink("PATCH", "{id}"),
        groupLink("DELETE", "{id}"),
        groupLink("POST", ""),
        groupUserLink("DELETE", "{groupId}", "{userId}"),
        groupUserLink("POST", "{groupId}", "{userId}")
      )
   links.map(link => LinksDTO(link._1, link._2.href)).toJson
  }
}


@Path("/groupsService")
@Api(value = "Groups Controller")
class GroupsController @Inject()(groupsDAO: GroupsDAO, userGroupsDAO: UserGroupsDAO, dbConfig: Db) extends JsonSupport with Links {

  lazy val log = LoggerFactory.getLogger(classOf[GroupsController])
  val newline = ByteString("\n")

  implicit val jsonStreamingSupport: JsonEntityStreamingSupport =
    EntityStreamingSupport.json()


  val defaultNumberOfGroupsOnPage = 20
  val defaultPageNumberForGroups = 1
  val maxPageSizeForGroups = 100
  implicit val ec: ExecutionContext = ExecutionContext.global

  val inject = Guice.createInjector(new DiModule())
  val service = inject.getInstance(classOf[GroupsService])

  @ApiOperation(value = "Get all paths for groups controller", httpMethod = "GET", response = classOf[GroupsDTO])
  @ApiResponses(Array(
    new ApiResponse(code = 400, message = "Bad request passed to the endpoint"),
    new ApiResponse(code = 200, message = "Step performed successfully")
  ))
  @Path("/")
  def getAllPaths: Route =
    pathEnd {
      get {
        complete {
          Marshal(getPaths).to[HttpResponse]
        }
      }
    }

  @ApiOperation(value = "Get all groups", httpMethod = "GET", response = classOf[GroupsDTO])
  @ApiResponses(Array(
    new ApiResponse(code = 400, message = "Bad request passed to the endpoint"),
    new ApiResponse(code = 200, message = "Step performed successfully")
  ))
  @Path("groups/all")
  def getAllGroups: Route =
    pathEnd {
      get {
        complete {
          service.getGroups map {
            case response => Marshal(toResources(response)).to[HttpResponse]
            case _ => Marshal(StatusCodes.NoContent).to[HttpResponse]
          }
        }
      }
    }

  @ApiOperation(value = "Get groups from particular page", httpMethod = "GET", response = classOf[GroupsDTO])
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "pageNumber", required = false, dataType = "number", paramType = "query", value = "page number (starts from 1)"),
    new ApiImplicitParam(name = "pageSize", required = false, dataType = "number", paramType = "query", value = "number of items shown per page")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 400, message = "Bad request passed to the endpoint"),
    new ApiResponse(code = 200, message = "Step performed successfully")
  ))
  @Path("/groups")
  def getGroupsFromPage: Route =
    pathEnd {
      get {
        parameterMultiMap { params =>
          val pageSize = params.get("pageSize").flatMap(_.headOption).map(_.toInt).getOrElse(0)
          val pageNumber = params.get("pageNumber").flatMap(_.headOption).map(_.toInt).getOrElse(0)
          if ((pageNumber > 0) && (pageSize > 0)) {
            if (pageSize > maxPageSizeForGroups) {
              complete{
                service.getGroupsFromPage(maxPageSizeForGroups, pageNumber).map {
                  case response => Marshal(toResourcesFromPage(response)).to[HttpResponse]
                  case _ => Marshal(StatusCodes.NoContent).to[HttpResponse]
                }
              }
            } else
              complete{
                service.getGroupsFromPage(pageSize, pageNumber).map {
                  case response => Marshal(toResourcesFromPage(response)).to[HttpResponse]
                  case _ => Marshal(StatusCodes.NoContent).to[HttpResponse]
                }
              }
          } else {
            complete{
              service.getGroupsFromPage(defaultNumberOfGroupsOnPage, defaultPageNumberForGroups).map {
                case response => Marshal(toResourcesFromPage(response)).to[HttpResponse]
                case _ => Marshal(StatusCodes.NoContent).to[HttpResponse]
              }
            }
          }
        }
      }
    }

  @ApiOperation(value = "Get group by Id", httpMethod = "GET", response = classOf[GroupsDTO])
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "id", required = true, dataType = "integer", paramType = "path", value = "Group Id")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 400, message = "Bad request passed to the endpoint"),
    new ApiResponse(code = 200, message = "Step performed successfully"),
    new ApiResponse(code = 204, message = "No group with such id was found")
  ))
  @Path("/groups/{id}")
  def getGroupById(@ApiParam(hidden = true) id: Int): Route =
    pathEnd {
      get {
        complete {
          (service.getGroupById(id)).map {
            case Some(response) => Marshal(toResource((response))).to[HttpResponse]
            case _ => Marshal(StatusCodes.NoContent).to[HttpResponse]
          }
        }
      }
    }

  @ApiOperation(value = "Get groups by Ids", httpMethod = "GET", response = classOf[GroupsDTO])
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "id", required = true, dataType = "integer", paramType = "path", value = "Group Id")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 400, message = "Bad request passed to the endpoint"),
    new ApiResponse(code = 200, message = "Step performed successfully"),
    new ApiResponse(code = 204, message = "No group with such id was found")
  ))
  @Path("/{id}")
  def getGroupsByIds(@ApiParam(hidden = true) id: Int): Route =
    pathEnd {
      get {
        complete {
          (service.getGroupById(id)).map {
            case Some(response) => Marshal(toResource((response))).to[HttpResponse]
            case _ => Marshal(StatusCodes.NoContent).to[HttpResponse]
          }
        }
      }
    }

  @ApiOperation(value = "Update group by Id", httpMethod = "PUT", response = classOf[GroupsDTO])
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "id", required = true, dataType = "integer", paramType = "path", value = "Group Id"),
    new ApiImplicitParam(name = "groupRow", required = true, dataType = "controller.GroupsDTO", paramType = "body", value = "Row to update group information")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 400, message = "Bad request passed to the endpoint"),
    new ApiResponse(code = 200, message = "Step performed successfully"),
    new ApiResponse(code = 204, message = "No group with such id was found")
  ))
  @Path("/groups/{id}")
  def updateGroupById(@ApiParam(hidden = true) id: Int): Route =
    pathEnd {
      put {
        entity(as[GroupsDTO]) { groupRow =>
          onComplete(service.updateGroupById(id, groupRow)) {
            case util.Success(Some(response)) => complete(StatusCodes.OK, response)
            case util.Success(None) => complete(StatusCodes.NoContent)
            case util.Failure(ex) => complete(StatusCodes.BadRequest, s"An error occurred: ${ex.getMessage}")
          }
        }
      }
    }

  @ApiOperation(value = "Update one field of group by Id", httpMethod = "PATCH", response = classOf[GroupsDTO])
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "id", required = true, dataType = "integer", paramType = "path", value = "Group Id"),
    new ApiImplicitParam(name = "field value", required = true, dataType = "controller.GroupsOptionDTO", paramType = "body", value = "Field name and value to update group information")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 400, message = "Bad request passed to the endpoint"),
    new ApiResponse(code = 200, message = "Step performed successfully"),
    new ApiResponse(code = 204, message = "No group with such id was found")
  ))
  @Path("/groups/{id}")
  def updateOneFieldInGroupById(@ApiParam(hidden = true) id: Int): Route =
    pathEnd {
      patch {
        entity(as[GroupsOptionDTO]) { fieldValue =>
          onComplete(service.updateOneFieldOfGroupById(id, fieldValue)) {
            case util.Success(Some(response)) => complete(StatusCodes.OK, response)
            case util.Success(None) => complete(StatusCodes.NoContent)
            case util.Failure(ex) => complete(StatusCodes.BadRequest, s"An error occurred: ${ex.getMessage}")
          }
        }
      }
    }

  @ApiOperation(value = "Delete group by Id", httpMethod = "DELETE", response = classOf[String])
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "id", required = true, dataType = "integer", paramType = "path", value = "Group Id")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 400, message = "Bad request passed to the endpoint"),
    new ApiResponse(code = 200, message = "Step performed successfully")
  ))
  @Path("/groups/{id}")
  def deleteGroup(id: Int): Route =
    pathEnd {
      delete {
        onComplete(service.deleteGroup(id)) {
          case util.Success(_) => complete(StatusCodes.OK)
          case util.Failure(ex) => complete(StatusCodes.NotFound, s"An error occurred: ${ex.getMessage}")
        }
      }
    }

  @ApiOperation(value = "Delete group for user", httpMethod = "DELETE", response = classOf[String])
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "groupId", required = true, dataType = "integer", paramType = "path", value = "Group Id"),
    new ApiImplicitParam(name = "userId", required = true, dataType = "integer", paramType = "path", value = "User Id")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 400, message = "Bad request passed to the endpoint"),
    new ApiResponse(code = 200, message = "Step performed successfully")
  ))
  @Path("/groups/{groupId}/users/{userId}")
  def deleteGroupForUser(groupId: Int, userId: Int): Route =
    pathEnd {
      delete {
        onComplete(service.deleteGroupForUser(userId, groupId)) {
          case util.Success(_) => complete(StatusCodes.OK)
          case util.Failure(ex) => complete(StatusCodes.NotFound, s"An error occurred: ${ex.getMessage}")
        }
      }
    }


  @ApiOperation(value = "Insert group", httpMethod = "POST", response = classOf[UsersDTO])
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "groupRow", required = true, dataType = "controller.GroupsDTO", paramType = "body", value = "Row to insert")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 400, message = "Bad request passed to the endpoint"),
    new ApiResponse(code = 201, message = "Step performed successfully")
  ))
  @Path("/groups")
  def insertGroup(): Route =
    pathEnd {
      post {
        entity(as[GroupsDTO]) { groupRow =>
          onComplete(service.insertGroup(groupRow)) {
            case util.Success(Some(response)) => complete(StatusCodes.Created, response)
            case util.Success(None) => complete(StatusCodes.BadRequest, s"User was not inserted")
            case util.Failure(ex) => complete(StatusCodes.BadRequest, s"An error occurred: ${ex.getMessage}")
          }
        }
      }
    }

  @ApiOperation(value = "Add group to user", httpMethod = "POST", response = classOf[String])
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "groupId", required = true, dataType = "integer", paramType = "path", value = "Group Id"),
    new ApiImplicitParam(name = "userId", required = true, dataType = "integer", paramType = "path", value = "User Id")

  ))
  @ApiResponses(Array(
    new ApiResponse(code = 400, message = "Bad request passed to the endpoint"),
    new ApiResponse(code = 200, message = "Step performed successfully")
  ))
  @Path("/groups/{groupId}/users/{userId}")
  def addGroupForUser(@ApiParam(hidden = true) groupId: Int, @ApiParam(hidden = true) userId: Int): Route =
    pathEnd {
      post {
        onComplete(service.addGroupToUser(userId, groupId)) {
          case util.Success(response) => {
            response match {
              case "" => complete(StatusCodes.OK)
              case _ => complete(StatusCodes.BadRequest, response)
            }
          }
          case util.Failure(ex) => complete(StatusCodes.BadRequest, s"An error occurred: ${ex.getMessage}")
        }
      }
    }

  @ApiOperation(value = "Get information about users for group with given id ", httpMethod = "GET", response = classOf[GroupWithUsersDTO])
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "id", required = true, dataType = "integer", paramType = "path", value = "Group Id")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 400, message = "Bad request passed to the endpoint"),
    new ApiResponse(code = 200, message = "Step performed successfully"),
    new ApiResponse(code = 204, message = "No user with such id was found")
  ))
  @Path("/groups/{id}/details")
  def getGroupDetails(@ApiParam(hidden = true) id: Int): Route =
    pathEnd {
      get {
        complete{
          service.getDetailsForGroup(id).map {
            case Some(response) => Marshal(toResourceDetails((response))).to[HttpResponse]
            case _ => Marshal(StatusCodes.NoContent).to[HttpResponse]
          }
        }
      }
    }

  lazy val groupRoutes: Route = {
    pathPrefix("groupsService") {
      getAllPaths ~
      pathPrefix("groups") {
        getGroupsFromPage ~
          insertGroup() ~
          pathPrefix("all") {
            getAllGroups
          } ~
          pathPrefix(IntNumber) { groupId =>
            getGroupById(groupId) ~
              updateGroupById(groupId) ~
              updateOneFieldInGroupById(groupId) ~
              deleteGroup(groupId) ~
              pathPrefix("users") {
                pathPrefix(IntNumber) { userId =>
                  deleteGroupForUser(userId, groupId) ~
                    addGroupForUser(groupId, userId)
                }
              } ~
              pathPrefix("details") {
                getGroupDetails(groupId)
              }
          }
      }
    }
  }
}
