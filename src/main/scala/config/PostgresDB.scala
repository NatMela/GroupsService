package config

import com.google.inject.Singleton
import javax.inject.Inject
import slick.jdbc.PostgresProfile
import slick.jdbc.PostgresProfile.api._

@Singleton
class PostgresDB @Inject() extends Db {
  val dbconf = Database.forConfig("usersGroups")

  def db(): PostgresProfile.backend.Database = {
    dbconf
  }
}
