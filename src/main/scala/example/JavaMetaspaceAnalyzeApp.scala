package example

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object JavaMetaspaceAnalyzeApp extends DataFrameOps with App {
  val spark = SparkSession.builder
    .master("local")
    .appName("Simple Application")
    .getOrCreate()

  val statsFile = "/Users/stan/Downloads/class_stats.txt" // Should be some file on your system

  /**
    |-- Index: integer (nullable = true)
    |-- Super: integer (nullable = true)
    |-- InstBytes: integer (nullable = true)
    |-- annotations: integer (nullable = true)
    |-- CpAll: integer (nullable = true)
    |-- MethodCount: integer (nullable = true)
    |-- MethodAll: integer (nullable = true)
    |-- KlassBytes: integer (nullable = true)
    |-- Bytecodes: integer (nullable = true)
    |-- ROAll: integer (nullable = true)
    |-- RWAll: integer (nullable = true)
    |-- Total: integer (nullable = true)
    |-- ClassName: string (nullable = true)
   */

  val df = spark.read
    .option("header", true)
    .option("inferSchema", true)
    .csv(statsFile)
    .cleanClassName(ClassName)
    .cache()

//  val count = df.count()
//  println(s"Items count ${count}")

  df.printSchema()

  df.agg(
    max(df.col(Bytecodes)),
    max(df.col(KlassBytes)),
    max(df.col(ROAll)),
    max(df.col(RWAll)),
    max(df.col(Total)),
    sum(df.col(Total))
  ).show

  df.topN(Total, 11)

  println("")
  println("")
  println("")

  val totalSum = s"$Total-sum"
  df.select(Total, ClassName)
    .groupBy(ClassName)
    .sum(Total)
    .sort(col(sumTotal).desc)
    .withIntFormattedColumn(sumTotal)
    .show(20, false)


  (0 to 10)
    .map(i => df.packageTotalLevel(i))
    .reduce(_.union(_))
    .union(df.allTotal())
//    .withIntFormattedColumn(sumTotal)
    .show(20, truncate = false)

  df
    .select(Total, ClassName)
    .findTopPackages(ClassName)
    .groupBy(ClassName)
    .sum(Total)
    .sort(col(sumTotal).desc)
    .withColumnRenamed(ClassName, "Package")
    .withIntFormattedColumn(sumTotal)
    .show(1000, false)

  spark.stop()
}


trait DataFrameOps {

  import scala.reflect.runtime.universe._

  val formatter = java.text.NumberFormat.getIntegerInstance
  val ClassName = "ClassName"
  val KlassBytes = "KlassBytes"
  val Bytecodes = "Bytecodes"
  val RWAll = "RWAll"
  val ROAll = "ROAll"
  val Total = "Total"
  val sumTotal = "sum(Total)"

  def tmp(s: String) = s"${s}_tmp"

  implicit class DataFrameOps(df: DataFrame) {
    def colToInt(colName: String): DataFrame = df
      .withColumn(tmp(colName), df.col(colName).cast(IntegerType))
      .drop(colName)
      .withColumnRenamed(tmp(colName), colName)

    def cleanClassName(colName: String): DataFrame = {
      // \$*
      val clean: String => String = _.replaceAll("\\d*", "")
      withUpdateColumn(colName, clean)
    }

    def withUpdateColumn[A, B](colName: String, fun: A => B)(implicit typeA: TypeTag[A], typeB: TypeTag[B]): DataFrame = {
      val updateUdf: UserDefinedFunction = udf(fun)
      df.withColumn(tmp(colName), updateUdf(df.col(colName)))
        .drop(colName)
        .withColumnRenamed(tmp(colName), colName)
    }
    def withIntFormattedColumn(colName: String): DataFrame =
      df.withUpdateColumn(colName, (i: Int) => formatter.format(i))

    def findTopPackages(colName: String): DataFrame = {
      (0 to 5)
        .map { d => df
          .filter(filterDotsCount(_ == d)(col(colName)))
          .withUpdateColumn(colName, cutPackageFun(d))
        }
        .reduce(_.union(_))
    }

    private def cutPackageFun(d: Int): String => String = _.split("\\.").take(d).mkString(".")

    private def filterDotsCount(dotsFun: Int => Boolean) = {
      val contain: String => Boolean = str => dotsFun(str.count(_ == '.'))
      udf(contain)
    }

    def showPackageLevel(d: Int, count: Int): Unit = {
      df
        .filter(filterDotsCount(_ == d)(col(ClassName)))
        .withUpdateColumn(Total, cutPackageFun(d))
        .show(count, false)
    }

    def packageTotalLevel(d: Int): DataFrame =
      df
        .filter(filterDotsCount(_ == d)(col(ClassName)))
        .agg(sum(col(Total)))
        .withColumn("Package", lit(s"Package level - $d"))

    def allTotal(): DataFrame =
      df
        .agg(sum(col(Total)))
        .withColumn("Package", lit(s"All packages"))

    def topN(colName: String, n: Int) = {
      df.sort(df.col(colName).desc).show(n)

      import org.apache.spark.sql.expressions.Window
      import org.apache.spark.sql.functions._

//      val groupeddf = df.groupBy(colName).agg(max(df.col(colName)))
//      groupeddf.show(n)

      // Window definition
//      val w = Window.partitionBy(df.col(colName)).orderBy(desc(colName))

      // Filter
//      df.withColumn("rank", rank.over(w)).where($"rank" <= n)

//      val windowSpec = Window.partitionBy(colName).orderBy(df.col("valueColName").desc)

//      groupeddf.withColumn("rank", rank().over(windowSpec))
//        .filter(col(valueColName) <= n)
//        .drop("rank")
    }
  }
}
