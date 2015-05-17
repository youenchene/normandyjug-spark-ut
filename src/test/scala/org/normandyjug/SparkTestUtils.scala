package org.normandyjug

import org.apache.log4j.Level
import org.apache.spark.SparkContext
import org.scalatest.FunSuite

object SparkTest extends org.scalatest.Tag("org.normandyjug.tags.SparkTest")


/**
 * Created by youen on 17/05/2015.
 */
trait SparkTestUtils extends FunSuite {

  var sc: SparkContext = _
  org.apache.log4j.Logger.getLogger("org").setLevel(Level.WARN)
  org.apache.log4j.Logger.getLogger("akka").setLevel(Level.WARN)
  org.apache.log4j.Logger.getLogger("Remoting").setLevel(Level.WARN)

  def sparkTest(name: String)(body: => Unit) {
    test(name, SparkTest){
      sc = new SparkContext("local[4]", name)
      try {
        body
      }
      finally {
        sc.stop
        sc = null
        // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
        System.clearProperty("spark.master.port")
      }
    }
  }

}
