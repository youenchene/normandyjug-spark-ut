package org.normandyjug

import org.apache.log4j.Level
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, ShouldMatchers}


/**
  * Created by youen on 16/05/2015.
  */
@RunWith(classOf[JUnitRunner])
class DataProcessWithSparkUtilsTest  extends SparkTestUtils with ShouldMatchers {

  sparkTest("should calculate average by category with Classic") {

     //GIVEN
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
   }

  sparkTest("should calculate average by category with SQL") {

     //GIVEN
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
   }

 }
