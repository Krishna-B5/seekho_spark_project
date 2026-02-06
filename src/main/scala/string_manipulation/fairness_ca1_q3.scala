package string_manipulation

import org.apache.spark.sql.functions.{col, regexp_extract, split}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

object fairness_ca1_q3 {

  def main(args:Array[String]): Unit ={

    val spark = SparkSession.builder()
      .appName(" Compensation Fairness")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    try{
      val df = List("A quick brown fox jumps over the lazy do").toDF("data")

      df.select(col("*"),
        split(col("data")," ").getItem(0).alias("1"),
        split(col("data")," ").getItem(1).alias("2"),
        split(col("data")," ").getItem(2).alias("3"),
        split(col("data")," ").getItem(3).alias("4"),
        split(col("data")," ").getItem(4).alias("5"),
        split(col("data")," ").getItem(5).alias("6"),
        split(col("data")," ").getItem(6).alias("7"),
        split(col("data")," ").getItem(7).alias("8"),
        split(col("data")," ").getItem(8).alias("9"),
        regexp_extract(col("data"),"^([A-za-z])",1).alias("1"),
        regexp_extract(col("data"),"\\s([A-za-z]+)",1).alias("2"),
        regexp_extract(col("data"),"\\s([A-za-z]+)+\\s([A-za-z]+)",2).alias("3"),
        regexp_extract(col("data"),"\\s([A-za-z]+)+\\s([A-za-z]+)+\\s([A-za-z]+)",3).alias("4"),
        regexp_extract(col("data"),"\\s([A-za-z]+)+\\s([A-za-z]+)+\\s([A-za-z]+)+\\s([A-za-z]+)",4).alias("5"),
        regexp_extract(col("data"),"\\s([A-za-z]+)+\\s([A-za-z]+)+\\s([A-za-z]+)+\\s([A-za-z]+)+\\s([A-za-z]+)",5).alias("6"),
        regexp_extract(col("data"),"\\s([A-za-z]+)+\\s([A-za-z]+)+\\s([A-za-z]+)+\\s([A-za-z]+)+\\s([A-za-z]+)+\\s([A-za-z]+)",6).alias("7"),
        regexp_extract(col("data"),"\\s([A-za-z]+)+\\s([A-za-z]+)+\\s([A-za-z]+)+\\s([A-za-z]+)+\\s([A-za-z]+)+\\s([A-za-z]+)+\\s([A-za-z]+)",7).alias("8"),
        regexp_extract(col("data"),"\\s([A-za-z]+)+\\s([A-za-z]+)+\\s([A-za-z]+)+\\s([A-za-z]+)+\\s([A-za-z]+)+\\s([A-za-z]+)+\\s([A-za-z]+)+\\s([A-za-z]+)",8).alias("9")
      ).show(false)
    } catch {
      case e: Exception => println("Error Message : "+e.getMessage)
    } finally {
      spark.stop()
    }
  }
}
