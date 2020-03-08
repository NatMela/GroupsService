package services

import java.sql.Date
import java.text.SimpleDateFormat
import java.time.LocalDate

import akka.stream.scaladsl.Source
import controller.{GroupWithUsersDTO, GroupsDTO, GroupsFromPage, GroupsOptionDTO, JsonSupport, UsersDTO}
import dao.{GroupsDAO, GroupsRow, GroupsTable, UserGroupsDAO, UsersAndGroupsRow}

import scala.concurrent.{Await, ExecutionContext, Future}
import config._
import com.google.inject.{Guice, Inject}
import diffson.lcs.Patience
import javax.jms.{Message, MessageListener, Session, TextMessage}
import org.apache.activemq.ActiveMQConnectionFactory
import org.slf4j.LoggerFactory
import slick.jdbc.PostgresProfile.api._
import spray.json.JsValue
import spray.json._
import diffson.sprayJson._
import diffson.diff
import diffson._
import diffson.jsonpatch.lcsdiff._

import scala.concurrent.duration.{Duration, MILLISECONDS}
import scala.util.{Failure, Success}

class UserListener(u: GroupsService) extends MessageListener with JsonSupport {
  override def onMessage(message: Message): Unit = {
    if (message.isInstanceOf[TextMessage]) {
      val s = message.asInstanceOf[TextMessage].getText
      s match {
        case msg if msg == "get_all_users" =>
          val result = Await.result(u.getGroupById(1), Duration(1000, MILLISECONDS))
          if (result.get.isInstanceOf[GroupsDTO]) {
            u.usersClient.send(u.session.createTextMessage(result.get.asInstanceOf[GroupsDTO].toString))
          }
        case _ =>
          Seq.empty[GroupsDTO]
      }
      println("Received text:" + s)
    } else {
      println("Received unknown")
    }
  }
}

class GroupsService @Inject()(groupsDAO: GroupsDAO, userGroupsDAO: UserGroupsDAO, dbConfig: Db) {

  lazy val log = LoggerFactory.getLogger(classOf[GroupsService])
  implicit val ec = ExecutionContext.global
  implicit val lcs = new Patience[JsValue]

  val maxGroupNumber = 16
  val connFactory = new ActiveMQConnectionFactory()
  val conn = connFactory.createConnection()
  val session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE)
  val destUsersServer = session.createQueue("users_server")
  val destUsersClient = session.createQueue("users_client")
  val destGroupsServer = session.createQueue("groups_server")
  val destGroupsClient = session.createQueue("groups_client")

  val usersServer = session.createConsumer(destUsersServer)
  val usersClient = session.createProducer(destUsersClient)
  val groupsServer = session.createProducer(destGroupsServer)
  val groupsClient = session.createConsumer(destGroupsClient)


  //  val groupsListener = new UserListener(this)

  conn.start()

  def convertStrToInt(str: String): Option[Int] = {
    try {
      val result = str.toInt
      Option(result)
    } catch {
      case e: Throwable => None
    }
  }

  val listener = Future {
    while (true) {
      val message = usersServer.receive()
      if (message.isInstanceOf[TextMessage]) {
        val s = message.asInstanceOf[TextMessage].getText
        s match {
          case msg if convertStrToInt(msg).isDefined =>
            val resultF = getGroupsForUser(msg.toInt)
            resultF.map(result => {
              usersClient.send(session.createTextMessage(result.toString))
            })
          case _ =>
            Seq.empty[UsersDTO]
        }
        println("Received text:" + s)
      } else {
        println("Received unknown")
      }
    }
  }


  listener.onComplete {
    case Success(value) => println(s"Got the callback, value = $value")
    case Failure(e) => println(s"D'oh! The task failed: ${e.getMessage}")
  }

  //  usersServer.setMessageListener(groupsListener)


  def getGroupsForUser(userId: Int): Future[Seq[GroupsDTO]] = {
    val groupsIdsForUserF = dbConfig.db.run(userGroupsDAO.getAllGroupsForUser(userId))

    groupsIdsForUserF.flatMap(groupsId => dbConfig.db.run(groupsDAO.getGroupsByIds(groupsId)).map {
      groupsRows =>
        groupsRows.map(groupRow => GroupsDTO(id = groupRow.id, title = groupRow.title, createdAt = Some(groupRow.createdAt.toString), description = groupRow.description))
    })
  }

  def getGroups: Future[Seq[GroupsDTO]] = {
    dbConfig.db().run(groupsDAO.getGroups()).map {
      groupsRows =>
        groupsRows.map(groupsRow =>
          GroupsDTO(id = groupsRow.id, title = groupsRow.title, createdAt = Some(groupsRow.createdAt.toString), description = groupsRow.description))
    }
  }

  def getGroupsFromPage(pageSize: Int, pageNumber: Int): Future[GroupsFromPage] = {
    val result = groupsDAO.getGroupsFromPage(pageNumber, pageSize)
    val numberOfAllGroupsF = dbConfig.db.run(result._2)
    val groupsOnPageF = dbConfig.db.run(result._1).map {
      groupRows =>
        groupRows.map(groupRow =>
          GroupsDTO(id = groupRow.id, title = groupRow.title, createdAt = Some(groupRow.createdAt.toString), description = groupRow.description))
    }
    val seqF = for {
      numberOfAllGroups <- numberOfAllGroupsF
      groupsOnPage <- groupsOnPageF
    } yield (numberOfAllGroups, groupsOnPage)
    seqF.map { result =>
      val (numberOfAllGroups, groupsOnPage) = result
      GroupsFromPage(groupsOnPage, numberOfAllGroups, pageNumber, pageSize)
    }
  }

  def getGroupById(groupId: Int): Future[Option[GroupsDTO]] = {
    dbConfig.db.run(groupsDAO.getGroupById(groupId)).map {
      groupRows =>
        groupRows.headOption match {
          case None => {
            log.info("There is no group with id {}", groupRows)
            None
          }
          case Some(groupRow) => {
            log.info("Group with id {} was found", groupId)
            Some(GroupsDTO(id = groupRow.id, title = groupRow.title, createdAt = Some(groupRow.createdAt.toString), description = groupRow.description))
          }
        }
    }
  }

  def getGroupsByIds(groupsId: Seq[Int]): Future[Seq[GroupsDTO]] = {
    dbConfig.db.run(groupsDAO.getGroupsByIds(groupsId)).map {
      groupsRows =>
        groupsRows.map(groupsRow =>
          GroupsDTO(id = groupsRow.id, title = groupsRow.title, createdAt = Some(groupsRow.createdAt.toString), description = groupsRow.description))
    }
  }

  def convertToDto(row: Seq[String]): UsersDTO = {
    println(row)
    UsersDTO(id = strToOptionInt(row(0).tail), firstName = row(1), lastName = row(2), createdAt = strToOptionDate(row(3)), isActive = strToBool(row(4).substring(0, row(4).length - 1)))
  }

  def strToOptionInt(str: String): Option[Int] = {
    str match {
      case string if string.substring(0, 4) == "Some" =>
        Option(string.substring(5, string.length - 1).toInt)
      case _ => None
    }
  }

  def strToBool(string: String): Boolean = {
    string match {
      case str if str == "true" => true
      case _ => false
    }
  }

  def strToOptionDate(str: String): Option[String] = {
    str match {
      case string if string.substring(0, 4) == "Some" =>
        Option(string.substring(5, string.length - 1))
      case _ => None
    }
  }

  def stringConvertToSetDTO(inputString: String, dtoName: String): Seq[UsersDTO] = {
    val setStrings: Seq[String] = inputString.substring(7, inputString.length - 1)
      .split(dtoName).toSeq.tail
    setStrings.map(row => row.split(",").toSeq)
      .map(row => convertToDto(row))
  }

  def getDetailsForGroup(groupId: Int): Future[Option[GroupWithUsersDTO]] = {
    val groupF: Future[Option[GroupsDTO]] = dbConfig.db.run(groupsDAO.getGroupById(groupId)).map {
      groupRows =>
        groupRows.headOption match {
          case None =>
            log.info("There is no group with id {}", groupId)
            None
          case Some(groupRow) => Some(GroupsDTO(id = groupRow.id, createdAt = Some(groupRow.createdAt.toString), title = groupRow.title, description = groupRow.description))
        }
    }

    val msg = session.createTextMessage(s"$groupId")
    groupsServer.send(msg)
    groupsClient.setMessageListener(null)
    log.info("sent message")
    println("sent message")
    val answer = groupsClient.receive(1000) //.asInstanceOf[TextMessage].getText
    log.info(s"receive answer ${answer}")

    val resultF = answer match {
      case message: TextMessage =>
        println(s"receive answer ${message.getText}")
        Future.successful(message.getText)

      case _ =>
        Future.successful("NotTextMessage")
    }

     val seqF = for {
       group <- groupF
       result <- resultF
     } yield (result, group)
     seqF.map { res =>
       val (result, group) = res
       val users = stringConvertToSetDTO(result,"UsersDTO")
       group match {
         case None =>
           log.warn("Can't ge details about the group as there is no group with id {}", groupId)
           None
         case Some(group) => {
           log.info("Details for group with id {} were found", groupId)
           Some(GroupWithUsersDTO(group, users.toSeq))
         }
       }
     }

  }

  def updateGroupById(groupId: Int, groupRow: GroupsDTO): Future[Option[GroupsDTO]] = {
    val groupF = getGroupById(groupId)
    groupF.flatMap {
      case Some(group) => {
        log.info("Group with id {} was found", groupId)
        val date = groupRow.createdAt.getOrElse(group.createdAt.get.toString)
        val rowToUpdate = GroupsRow(id = Some(groupId), title = groupRow.title, createdAt = java.sql.Date.valueOf(date), description = groupRow.description)
        dbConfig.db.run(groupsDAO.update(rowToUpdate)).flatMap(_ => getGroupById(groupId))
      }
      case None => {
        log.warn("Can't update group info, because there is no group with id {}", groupId)
        Future.successful(None)
      }
    }
  }

  def updateOneFieldOfGroupById(groupId: Int, valueToUpdate: GroupsOptionDTO): Future[Option[GroupsDTO]] = {
    val groupF = getGroupById(groupId)
    groupF.flatMap {
      case Some(group) => {
        log.info("Group with id {} was found", groupId)
        val rowToUpdate = GroupsRow(id = Some(groupId), title = valueToUpdate.title.getOrElse(group.title), createdAt = java.sql.Date.valueOf(valueToUpdate.createdAt.getOrElse(group.createdAt.get.toString)), description = valueToUpdate.description.getOrElse(group.description))
        dbConfig.db.run(groupsDAO.update(rowToUpdate)).flatMap(_ => getGroupById(groupId))
      }
      case None => {
        log.warn("Can't update group info, because there is no group with id {}", groupId)
        Future.successful(None)
      }
    }
  }

  def insertGroup(group: GroupsDTO): Future[Option[GroupsDTO]] = {
    val insertedGroup = GroupsRow(id = group.id, title = group.title, createdAt = java.sql.Date.valueOf(LocalDate.now), description = group.description)
    val idF = dbConfig.db.run(groupsDAO.insert(insertedGroup))
    idF.flatMap { id =>
      dbConfig.db.run(groupsDAO.getGroupById(id)).map {
        groupRows =>
          groupRows.headOption match {
            case None =>
              log.warn("Group was not added")
              None
            case Some(groupRow) => {
              log.info("Group with id {} was created", groupRow.id)
              Some(GroupsDTO(id = groupRow.id, title = groupRow.title, createdAt = Some(groupRow.createdAt.toString), description = groupRow.description))
            }
          }
      }
    }
  }

  def isUserAlreadyInGroup(userId: Int, groupId: Int) = {
    val userGroupRowF = dbConfig.db.run(userGroupsDAO.getUserGroupRow(userId, groupId))
    userGroupRowF.map(userGroupRow =>
      if (userGroupRow.nonEmpty) true else false)
  }

  def couldWeAddGroupForUser(userId: Int) = {
    val groupsForUserF = dbConfig.db.run(userGroupsDAO.getAllGroupsForUser(userId))
    groupsForUserF.map(groupsForUser =>
      if (groupsForUser.size < maxGroupNumber) true else false)
  }

  def needToAddUserToGroup(userId: Int, groupId: Int) = {
    val seqF = for {
      isUserInGroup <- isUserAlreadyInGroup(userId, groupId)
      couldWeAddGroup <- couldWeAddGroupForUser(userId)
    } yield (isUserInGroup, couldWeAddGroup)
    seqF.map { result =>
      val (isUserInGroup, couldWeAddGroup) = result
      if (!isUserInGroup && couldWeAddGroup)
        true
      else
        false
    }
  }

  def addGroupToUser(userId: Int, groupId: Int): Future[String] = {
    val groupF = getGroupById(groupId)
    /*dbConfig.db.run(userDAO.getUserById(userId)).flatMap(userRows =>
      userRows.headOption match {
        case Some(user) => {
          groupF.flatMap {
            case Some(_) => {
              if (user.isActive) {
                needToAddUserToGroup(userId, groupId).flatMap { needToAdd =>
                  if (needToAdd) {
                    val rowToInsert = UsersAndGroupsRow(None, userId, groupId)
                    log.info("Add user with id {} to group with id {} ", userId, groupId)
                    dbConfig.db.run(userGroupsDAO.insert(rowToInsert))
                    Future.successful("")
                  } else {
                    log.warn(s"Group was not added because user is already in group or user have already included in $maxGroupNumber groups")
                    Future.successful(s"Group was not added because user with id $userId is already in group with id $groupId or user have already included in $maxGroupNumber groups")
                  }
                }
              } else {
                log.warn("Group was not added because user is nonActive")
                Future.successful(s"Group was not added because user with id $userId is nonActive")
              }
            }
            case None => {
              log.warn("Group was not added because there is no group with id {}", groupId)
              Future.successful(s"Group was not added because there is no group with id $groupId")
            }
          }
        }
        case None => {
          log.warn("Group was not added because there is no user with id {}", userId)
          Future.successful(s"Group was not added because there is no user with id $userId")
        }
      })*/
    Future.successful("Result")
  }

  def deleteGroup(groupId: Int): Future[Unit] = {
    getGroupById(groupId).map {
      case Some(_) => val query = DBIO.seq(groupsDAO.delete(groupId), userGroupsDAO.deleteUsersFromGroup(groupId)).transactionally
        dbConfig.db().run(query)
        val message = s"Group with id $groupId is deleted"
        log.info(message)
      case None => val message = s"Group with id $groupId is not found"
        log.info(message)
    }
  }

  def deleteGroupForUser(userId: Int, groupId: Int): Future[Unit] = {
    dbConfig.db().run(userGroupsDAO.deleteRowForParticularUserAndGroup(userId, groupId))
    val message = s"User with id $userId is deleted from group with $groupId"
    log.info(message)
    Future.successful()
  }
}