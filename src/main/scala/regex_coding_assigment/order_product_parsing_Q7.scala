package regex_coding_assigment

import org.apache.spark.sql.functions.{col, countDistinct, lower, regexp_extract, substring, sum}
import org.apache.spark.sql.{DataFrame, SparkSession}

object order_product_parsing_Q7 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName(" Order")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    try {

      val data = List(("ORD001", "P123-4567", "2024-08-01", "Delivered"),
        ("ORD002", "P234-5678", "2024-08-05", "Pending"),
        ("ORD003", "P345-6789", "2024-08-10", "Delivered"))

      val df = spark.createDataFrame(data)
      val df1 = df.toDF("order_id", "product_code", "order_date", "delivery_status")

      val reg = regexp_extract(col("order_id"), "([0-9]+)", 1)

      def product_order(df2: DataFrame): DataFrame = {
        val result = df2.dropDuplicates("order_id")
          .select(col("*"),
            substring(col("order_id"), 1, 3).alias("order_3"),
            substring(col("product_code"), 6, 4).alias("product_4"),
            lower(col("delivery_status")).alias("delivery_status_l"))
          //        .filter(!col("order_3").startsWith("ORD") && col("delivery_status_l").equals("delivered") )
          .groupBy(col("delivery_status_l")).agg(
            sum(reg).alias("sum_order_id"),
            countDistinct(col("delivery_status_l")).alias("delivery_status_c")
          )

        result
      }
      product_order(df1).show()
      println("SQL Logic")
      df1.createOrReplaceTempView("sample")
      spark.sql(
        """
        with dedup as(
        select order_id, product_code, order_date, delivery_status
        from sample
        group by order_id, product_code, order_date, delivery_status
        ),
        derived as (
        select order_id, product_code, order_date, delivery_status,
        lower(delivery_status) as delivery_status_l,
        substring(order_id,1,3) as order_3,
        substring(product_code,6,4) as product_4
        from dedup
        )
        select
        delivery_status_l,
        sum(regexp_extract(order_id,"([0-9]+)",1)) as sum_order_id,
        count(distinct delivery_status_l) as delivery_status_c
        from derived
        group by delivery_status_l
        """).show()


    } catch {
      case e: Exception => println(" Error Message : ")
    } finally {
      spark.stop()
    }
  }
}
