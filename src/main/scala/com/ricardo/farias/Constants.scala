package com.ricardo.farias

import com.typesafe.config.ConfigFactory

object Constants {
  private val prop = ConfigFactory.load

  val env: String = prop.getString("environment")
  val master: String = prop.getString(s"${env}.master")
  val appName: String = prop.getString(s"${env}.appName")
  val directory: String = prop.getString(s"${env}.directory")
  val bucket: String = prop.getString(s"${env}.bucketName")
  val database: String = prop.getString(s"${env}.database")
}
