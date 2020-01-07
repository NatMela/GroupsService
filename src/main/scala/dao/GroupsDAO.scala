package dao

import java.sql.Date

import com.google.inject.Singleton
import javax.inject.Inject
import slick.jdbc.PostgresProfile.api._
import slick.lifted.Shape
import slick.sql.FixedSqlAction

import scala.concurrent.ExecutionContext

case class GroupsRow(id: Option[Int], title: String, createdAt: Date, description: String)

class GroupsTable(tag: Tag) extends Table[GroupsRow](tag, "groups") {

  def id = column[Int]("id", O.PrimaryKey, O.AutoInc)

  def title = column[String]("title")

  def createdAt = column[Date]("created_at")

  def description = column[String]("description")

  def * = (id.?, title, createdAt, description) <> (GroupsRow.tupled, GroupsRow.unapply)

}

@Singleton
class GroupsDAO @Inject()() {

  val allGroups = TableQuery[GroupsTable]

  implicit val ec = ExecutionContext.global

  def getGroups() = {
    allGroups.result
  }

  def getGroupById(groupId: Int) = {
    val group = allGroups.filter(_.id === groupId)
    group.result
  }

  def getGroupsByIds(groupIds: Seq[Int]) = {
    val groups = allGroups.filter(_.id inSet groupIds)
    groups.result
  }

  def getGroupsFromPage(pageNumber: Int, pageSize: Int) = {
    val startNumberOfNeededUsers = (pageNumber - 1) * pageSize
    val skipPages = allGroups.drop(startNumberOfNeededUsers)
    val groupsFromPage = skipPages.take(pageSize)
    val numberOfAllGroups = allGroups.size
    (groupsFromPage.result, numberOfAllGroups.result)
  }

  def update(group: GroupsRow) = {
    allGroups.filter(_.id === group.id).update(group)
  }

  def delete(id: Int) = {
    allGroups.filter(_.id === id).delete
  }

  def insert(group: GroupsRow) = {
    (allGroups returning allGroups.map(_.id)) += group
  }
}
