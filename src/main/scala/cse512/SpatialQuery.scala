package cse512

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQueryListener.QueryTerminatedEvent

object SpatialQuery extends App{
  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>(st_Contains(queryRectangle, pointString)))

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()

    return resultDf.count()
  }
  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>(st_Contains(queryRectangle, pointString)))

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>(st_Withins(pointString1, pointString2, distance)))

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>(st_Withins(pointString1, pointString2, distance)))
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }
  def st_Contains(queryRectangle:String, pointString: String):Boolean = {
    val listOfRectanglePoints = queryRectangle.split(",").map(s => s.toDouble)
    val listOfPoints = pointString.split(",").map(s => s.toDouble)
    val rectangleLowerX = math.min(listOfRectanglePoints(0), listOfRectanglePoints(2))
    val rectangleLowerY = math.min(listOfRectanglePoints(1), listOfRectanglePoints(3))
    val rectangleUpperX = math.max(listOfRectanglePoints(0), listOfRectanglePoints(2))
    val rectangleUpperY = math.max(listOfRectanglePoints(1), listOfRectanglePoints(3))
    if (listOfPoints(0) >= rectangleLowerX && listOfPoints(0) <= rectangleUpperX && rectangleLowerY <= listOfPoints(1) && listOfPoints(1) <= rectangleUpperY) {return true} else {return false}
  }
  def st_Withins(pointString1: String, pointString2: String, distance: Double): Boolean ={
    val listOfPoints1 = pointString1.split(",").map(s => s.toDouble)
    val listOfPoints2 = pointString2.split(",").map(s => s.toDouble)
    val actualDistance = math.sqrt(math.pow(listOfPoints1(0) - listOfPoints2(0), 2) + math.pow(listOfPoints1(1) - listOfPoints2(1), 2))
    if (actualDistance <= distance) {return true}
    else {return false}
  }

}
