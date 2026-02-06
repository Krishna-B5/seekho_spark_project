package how_to_select

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, desc, max, row_number}

object win_class_pgm {

  def main(args:Array[String]): Unit ={

    val spark = SparkSession.builder()
      .appName("CLass pgm")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    try{

      val salesData = Seq(
        ("Product1", "Category1", 100),
        ("Product2", "Category2", 200),
        ("Product3", "Category1", 150),
        ("Product4", "Category3", 300),
        ("Product5", "Category2", 450),
        ("Product6", "Category3", 180))

      val df = spark.createDataFrame(salesData)
      val df1 = df.toDF("Product","Category","Revenue")

      val window = Window.partitionBy(col("Category")).orderBy(col("Revenue").desc)

      val df2 = df1.select(col("*"),
        max(col("Revenue")).over(window).alias("max_revenue"),
        row_number().over(window).alias("max_value") )
        df2.show()
        val df3 = df2.filter(col("max_value") === 2)
      df3.show()
    }catch {
      case e: Exception => println("Error Message : "+e.getMessage)
    } finally {
      spark.stop()
    }




  }
}
