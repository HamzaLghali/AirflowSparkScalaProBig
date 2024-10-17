package com

import org.json4s._
import org.json4s.native.JsonMethods._
import scalaj.http._


object JsonManip extends App {

  implicit val formats = DefaultFormats
  val response: HttpResponse[String] = Http("https://dummyjson.com/users").asString
  val json = parse(response.body)
  val firstResult = (json \ "users")(0)

  val seq =Seq("id", "firstName", "lastName", "maidenName", "age", "gender", "email", "phone", "username" ,"hair")

var i : Int =0
   while(i<10){
     if(seq(i) =="hair") {
      val seqr =Seq("color","type")
       val ref = (firstResult \ seq(i) \ seqr(0)).extract[String]
       val rer = (firstResult \ seq(i) \ seqr(1)).extract[String]
       println(s"Hair : $ref,$rer")
       i=i+1
     }else {
       val re = (firstResult \ seq(i)).extract[String]
       println(re)
       i=i+1
     }
   }




}
