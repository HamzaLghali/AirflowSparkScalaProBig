package com

import org.json4s.DefaultFormats
import org.json4s.native.JsonMethods.parse
import scalaj.http.{Http, HttpResponse}

object RandomUser extends App{



  implicit val formats = DefaultFormats
  val response: HttpResponse[String] = Http("https://dummyjson.com/users").asString
  val json = parse(response.body)

  val seq =Seq("id","firstName","lastName","maidenName","age","gender","email","phone","username","password","birthDate","image","bloodGroup","height","weight","eyeColor","ip","macAddress","university" )
  var i : Int = 0
  var j: Int = 0

  while (j<2){
    val firstResult = (json \ "users")(j)

      for (field <- seq) {
          val value = (firstResult \ field)

          val extractedValue = value.extractOpt[String].getOrElse(value.extractOpt[Int].getOrElse("Unknown"))
          println(s"$field: $extractedValue")
        }
      j=j+1
  }
}

