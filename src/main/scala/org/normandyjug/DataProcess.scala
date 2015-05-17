package org.normandyjug

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.rdd.RDD


case class Item(ref: String, price: Double, category: String)

/**
 * Created by youen on 16/05/2015.
 */
object DataProcess {


  def classicProcess(itemsRdd: RDD[Item]): RDD[(String,Double)] = {
   val p=itemsRdd.map(item => (item.category,item.price)).mapValues(x=>(x,1)).reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))
    p.mapValues(x=>(x._1 / x._2));
  }

  def sqlProcess(sc: SparkContext,itemsRdd: RDD[Item]): DataFrame = {
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val itemsDF=itemsRdd.toDF()
    itemsDF.registerTempTable("items")
    sqlContext.sql("SELECT category, AVG(price) FROM items group by category")
  }

}
