package dao

import com.google.inject.Singleton
import slick.jdbc.PostgresProfile.api._

import scala.concurrent.ExecutionContext
import javax.inject.Inject

case class UsersAndGroupsRow(userGroupId: Option[Int], userId: Int, groupId: Int)

class UsersAndGroupsTable(tag: Tag) extends Table[UsersAndGroupsRow](tag, "users_groups") {

  def userGroupId = column[Int]("user_group_id", O.PrimaryKey, O.AutoInc)

  def userId = column[Int]("user_id")

  def groupId = column[Int]("group_id")

  def * = (userGroupId.?, userId, groupId) <> (UsersAndGroupsRow.tupled, UsersAndGroupsRow.unapply)

}

@Singleton
class UserGroupsDAO @Inject()() {
  implicit val executionContext = ExecutionContext.global
  val allRows = TableQuery[UsersAndGroupsTable]
  val groupsRows = TableQuery[GroupsTable]
  val groupsDao = new GroupsDAO

  def getAllGroupsForUser(userId: Int) = {
    allRows.filter(_.userId === userId).map(_.groupId).result
  }

  def getAllUsersForGroup(groupId: Int) = {
    allRows.filter(_.groupId === groupId).map(_.userId).result
  }

  def deleteRowForParticularUserAndGroup(userId: Int, groupId: Int) = {
    allRows.filter(_.userId === userId).filter(_.groupId === groupId).delete
  }

  def insert(usersAndGroupsRow: UsersAndGroupsRow) = {
    (allRows returning allRows.map(_.userGroupId)) += usersAndGroupsRow
  }

  def getById(userGroupsId: Int) = {
    allRows.filter(_.userGroupId === userGroupsId).result
  }

  def deleteGroupsForUser(userId: Int) = {
    allRows.filter(_.userId === userId).delete
  }

  def deleteUsersFromGroup(groupId: Int) = {
    allRows.filter(_.groupId === groupId).delete
  }

  def getUserGroupRow(userId: Int, groupId: Int) = {
    allRows.filter(_.userId === userId).filter(_.groupId === groupId).result
  }

  //TODO transactions
  def getGroupsForUsers(userId: Int) = {
    val query = (for {
      groupsId <- allRows.filter(_.userId === userId).map(_.groupId).result
      _ <- DBIO.seq(groupsId.map(groupId => groupsRows.filter(_.id === groupId).result): _*)
    } yield ()).transactionally
    query
  }
}
