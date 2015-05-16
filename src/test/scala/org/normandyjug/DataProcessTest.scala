package org.normandyjug

import org.apache.log4j.Level
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, ShouldMatchers}


/**
 * Created by youen on 16/05/2015.
 */
@RunWith(classOf[JUnitRunner])
class DataProcessTest extends FunSuite with ShouldMatchers {

  var sc: SparkContext = _
  org.apache.log4j.Logger.getLogger("org").setLevel(Level.WARN)
  org.apache.log4j.Logger.getLogger("akka").setLevel(Level.WARN)
  org.apache.log4j.Logger.getLogger("Remoting").setLevel(Level.WARN)


  test("should calculate average by category with Classic") {

    //GIVEN
    sc = new SparkContext("local[4]", "test")

    try {

      val items: RDD[Item] = sc.parallelize(List(
      "Carrot,0.70,FOOD",
        "Potatoes,0.45,FOOD",
        "BORDEAUX,1,DRINK"))
        .map(_.split(","))
        .map(e => Item(e(0), e(1).toDouble, e(2)))

      //WHEN
      val result=DataProcess.classicProcess(items).collect();

      //THEN
      result should have size (2)
      result should contain ("DRINK",1)
      result should contain  ("FOOD",((0.45+0.70)/2))

    } finally {
      if(sc != null)
        sc.stop()
    }
  }

  test("should calculate average by category with SQL") {

    //GIVEN
    sc = new SparkContext("local[4]", "test")

    try {

      val items: RDD[Item] = sc.parallelize(List(
        "Carrot,0.70,FOOD",
        "Potatoes,0.45,FOOD",
        "BORDEAUX,1,DRINK",
        "Boxes,2,STUFF",
        "Things,4,STUFF"))
        .map(_.split(","))
        .map(e => Item(e(0), e(1).toDouble, e(2)))

      //WHEN
      val result=DataProcess.sqlProcess(sc,items).collect();

      //THEN
      result should have size (3)
      result should contain (Row("DRINK",1))
      result should contain (Row("STUFF",3))
      result should contain (Row("FOOD",((0.45+0.70)/2)))

    } finally {
      if(sc != null)
        sc.stop()
    }
  }

}
