package com

import cats.effect._
import cats.effect.std.Console
import fs2.io.net.Network
import natchez.Trace
import natchez.Trace.Implicits.noop

import java.util.UUID
import pureconfig.{ConfigSource}
import skunk.Session
import skunk.syntax.all._
import skunk.codec.all._



final case class Config(

                         host : String,
                         port : Int,
                         username : String,
                         password: String,
                         database : String
                       )

final case class User(
                       id : UUID,
                       name : String,
                       email : String
                     )

object postskunk extends IOApp.Simple {

  def getSession[F[_] : Temporal : Trace : Network : Console](config: Config) =
    Session.single(
      host = config.host,
      port = config.port,
      user = config.username,
      password = Some(config.password),
      database = config.database
    )

  override def run =
    ConfigSource.default.at("db").load[Config] match {
      case Left(errors) =>
        IO(println(errors))
      case Right(config) =>
        getSession[IO](config) use { session =>
          for {
            _ <- IO.println("opening a session")
            timestamp <- session.unique(sql"select current_timestamp".query(timestamptz))
          } yield ()

        }
    }

}