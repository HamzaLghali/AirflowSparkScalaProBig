package com

import org.json4s._
import org.json4s.native.JsonMethods._
import scalaj.http._


object JsonManip extends App {


  //val res = requests.get("https://randomuser.me/api/")

  // val response: HttpResponse[String] = Http("https://randomuser.me/api/").asString
  //
  // // Print the response details
  // println("Response Code: " + response.code)       // Get status code
  // println("Response Body: " + response.body)       // Get response body as string
  // println("Response Headers: " + response.headers) // Get response headers
  // println("Response Cookies: " + response.cookies) // Get cookies (if any)

  implicit val formats = DefaultFormats // Needed for JSON parsing

  val response: HttpResponse[String] = Http("https://randomuser.me/api/").asString

  // Parse the JSON response
  val json = parse(response.body)

  // Access the first element of the "results" array
  val firstResult = (json \ "results")(0)

  // Extract fields from the first result
  val gender = (firstResult \ "gender").extract[String]
  val firstName = (firstResult \ "name" \ "first").extract[String]
  val lastName = (firstResult \ "name" \ "last").extract[String]
  val email = (firstResult \ "email").extract[String]
  val location = (firstResult \ "location" \ "street" \ "number").extract[String]

  println(s"Gender: $gender")
  println(s"Name: $firstName $lastName")
  println(email)
  println(location)

}
