package how_to_select

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, count, floor, max, min, sum}

object group_by_agg_class {

  def main(args:Array[String]): Unit ={

    val spark = SparkSession.builder()
      .appName("groupby agg class")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val orderData = Seq(
      ("Order1", "John", 100),
      ("Order2", "Alice", 200),
      ("Order3", "Bob", 150),
      ("Order4", "Alice", 300),
      ("Order5", "Bob", 250),
      ("Order6", "John", 400)
    ).toDF("OrderID", "Customer", "Amount")

    val df = orderData.groupBy(col("customer")).agg(sum(col("amount")),count(col("OrderID")))
    df.show()

    val scoreData = Seq(
      ("Alice", "Math", 80),
      ("Bob", "Math", 90),
      ("Alice", "Science", 70),
      ("Bob", "Science", 85),
      ("Alice", "English", 75),
      ("Bob", "English", 95)
    ).toDF("Student", "Subject", "Score")

    val df1 = scoreData.groupBy(col("Subject")).agg(avg(col("Score")))
    df1.show()
    val df2 = scoreData.groupBy(col("Student")).agg(max(col("Score")))
    df2.show()

    val weatherData = Seq(
      ("City1", "2022-01-01", 10.0),
      ("City1", "2022-01-02", 8.5),
      ("City1", "2022-01-03", 12.3),
      ("City2", "2022-01-01", 15.2),
      ("City2", "2022-01-02", 14.1),
      ("City2", "2022-01-03", 16.8)
    ).toDF("City", "Date", "Temperature")

    val df3 = weatherData.groupBy(col("City")).agg(min("Temperature"), max(col("Temperature")),floor(avg(col("Temperature"))))
    df3.show()

  }



}
