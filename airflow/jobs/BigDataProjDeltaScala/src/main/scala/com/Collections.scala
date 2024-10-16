package com.Collections

/**
 * Examples for working with Scala collections.
 */
object Collections extends App {

  def mapValues: Unit = {
    val m = Map("a" -> 1, "b" -> 2, "c" -> 3)

    // Native approach, inefficient  since .toList, .map, .toMap each creates a copy
    m.toList.map(t => (t._1, t._2 + 1)).toMap

    // Better, one copy
    m.map(kv => (kv._1, kv._2 + 1))

    // Lazy version, no copy
    m.mapValues(_ + 1)
  }

  def mergeMaps: Unit = {
    val m1 = Map("a" -> 1.0, "b" -> 2.0, "c" -> 3.0)
    val m2 = Map("a" -> 1.5, "b" -> 2.5, "d" -> 3.5)

    // Native approach, inefficient since it creates many copies
    val i = m1.keySet intersect m2.keySet
    val m = i.map(k => k -> (m1(k) + m2(k))) // sum values of common keys
    (m1 -- i) ++ (m2 -- i) ++ m // inefficient, creates 2 more temporary maps
    m1 ++ m2 ++ m // slightly better, values from RHS overwrites those from LHS

    // Slightly better but still creates a temporary set
    (m1.keySet ++ m2.keySet).map(k => k -> (m1.getOrElse(k, 0.0) + m2.getOrElse(k, 0.0)))

    // Better but slightly cryptic
    m1 ++ m2.map { case (k, v) => k -> (v + m1.getOrElse(k, 0.0)) }
  }

  def listToMap: Unit = {
    val l = List(1, 2, 3, 4, 5)

    // Native approach, creates a temporary copy
    l.map(x => "key" + x -> x).toMap

    // Slightly better, using a mutable builder
    val b = Map.newBuilder[String, Int]
    l.foreach(x => b += "key" + x -> x)
    b.result()

    // Use implicits to automatically build for the target collection type Map[String, Int]
    val m: Map[String, Int] = l.map(x => "key" + x -> x)(scala.collection.breakOut)
  }



  listToMap
}