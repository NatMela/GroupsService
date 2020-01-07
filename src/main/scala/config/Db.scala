package config

import slick.jdbc.PostgresProfile

abstract class Db {
  def db(): PostgresProfile.backend.Database
}
