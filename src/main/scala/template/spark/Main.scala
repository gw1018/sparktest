package template.spark

import magellan.{Point, Polygon, PolyLine}
import magellan.coord.NAD83
import org.apache.spark.sql.magellan._
import org.apache.spark.sql.magellan.dsl.expressions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import org.apache.spark.sql.functions._

final case class CaseRecord(caseId: String, timestamp: String, point: Point)

object Main extends InitSpark {

  def main(args: Array[String]) = {
    import spark.implicits._

    val version = spark.version
    println("SPARK VERSION = " + version)

    val bcase = sc.textFile("bc.csv").map { line =>
      val parts = line.split("," )
      val caseId = parts(0)
      val timestamp = parts(1)
      val point = Point(parts(14).toDouble, parts(15).toDouble)
      CaseRecord(caseId, timestamp, point)
    }.repartition(100).toDF().cache()

    bcase.show(5)

    val neighborhoods = sqlContext.read.format("magellan")
      .load("SFNbhd/")
      .select($"polygon", $"metadata")
      .cache()

    println(neighborhoods.count()) // how many neighbourhoods in SF?

    neighborhoods.printSchema

    neighborhoods.select(explode($"metadata").as(Seq("k", "v"))).show(1,true)

    val joined = neighborhoods
      .join(bcase)
      .where($"point" within $"polygon")
      .select($"caseId", $"timestamp", explode($"metadata").as(Seq("k", "v")))
      .withColumnRenamed("v", "County")
      .drop("k")
      .cache()

    joined
      .groupBy($"County")
      .agg(countDistinct("caseId")
        .as("cases"))
      .orderBy(col("cases").desc)
      .show(100,false)

    close
  }
}
