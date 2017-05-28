package template.spark

import magellan.{Point, Polygon, PolyLine}
import magellan.coord.NAD83
import org.apache.spark.sql.magellan._
import org.apache.spark.sql.magellan.dsl.expressions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import org.apache.spark.sql.functions._

final case class Person(firstName: String, lastName: String,
                        country: String, age: Int)

final case class UberRecord(tripId: String, timestamp: String, point: Point)

object Main extends InitSpark {

  def main(args: Array[String]) = {
    import spark.implicits._

    val version = spark.version
    println("SPARK VERSION = " + version)

//    val sumHundred = spark.range(1, 101).reduce(_ + _)
//    println(f"Sum 1 to 100 = $sumHundred")
//
//    println("Reading from csv file: people-example.csv")
//    val persons = reader.csv("people-example.csv").as[Person]
//    persons.show(2)
//    val averageAge = persons.agg(avg("age"))
//      .first.get(0).asInstanceOf[Double]
//    println(f"Average Age: $averageAge%.2f")

    val uber = sc.textFile("all.tsv").map { line =>
      val parts = line.split("\t" )
      val tripId = parts(0)
      val timestamp = parts(1)
      val point = Point(parts(3).toDouble, parts(2).toDouble)
      UberRecord(tripId, timestamp, point)
    }.repartition(100).toDF().cache()

     uber.show(5)

//    val points = sc.parallelize(Seq((-1.0, -1.0), (-1.0, 1.0), (1.0, -1.0))).toDF("x", "y").select(point($"x", $"y").as("point"))
//
//    points.show()

//        val magellanContext = new MagellanContext(sc)
//        val neighborhoods = magellanContext.read.format("magellan").
//          load(${neighborhoods.path}).
//          select($"polygon", $"metadata").
//          cache()

    //    neighborhoods.
    //      join(uber).
    //      where($"point" within $"polygon").
    //      select($"tripId", $"timestamp", explode($"metadata").as(Seq("k", "v"))).
    //      withColumnRenamed("v", "neighborhood").
    //      drop("k").
    //      show(5)

    val neighborhoods = sqlContext.read.format("magellan")
      .load("SFNbhd/")
      .select($"polygon", $"metadata")
      .cache()

    println(neighborhoods.count()) // how many neighbourhoods in SF?

    neighborhoods.printSchema

//    neighborhoods.take(2) // see the first two neighbourhoods

    neighborhoods.select(explode($"metadata").as(Seq("k", "v"))).show(5,false)

//    val transformer: Point => Point = (point: Point) =>
//    {
//      val from = new NAD83(Map("zone" -> 403)).from()
//      val p = point.transform(from)
////      p.setX((3.28084 * p.getX()))
////      p.setY(3.28084 * p.getY())
//      new Point()
////        newp.setX(3.28084 * p.getX())
////        newp.setY(3.28084 * p.getY())
//
//    }

    val transformer: Point => Point = (point: Point) => {
      val from = new NAD83(Map("zone" -> 403)).from()
      val p = point.transform(from)
      val np = new Point()
      np.setY(3.28084 * p.getY())
      np.setX(3.28084 * p.getX())
      np
    }

    // add a new column in nad83 coordinates
    val uberTransformed = uber
      .withColumn("nad83", $"point".transform(transformer))
      .cache()

    val joined = neighborhoods
      .join(uberTransformed)
      .where($"nad83" within $"polygon")
      .select($"tripId", $"timestamp", explode($"metadata").as(Seq("k", "v")))
      .withColumnRenamed("v", "neighborhood")
      .drop("k")
      .cache()

    joined
      .groupBy($"neighborhood")
      .agg(countDistinct("tripId")
        .as("trips"))
      .orderBy(col("trips").desc)
      .show(5,false)

    close
  }
}
