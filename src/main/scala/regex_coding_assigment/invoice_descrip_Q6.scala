package regex_coding_assigment

import org.apache.spark.sql.functions.{avg, col, regexp_extract, sum, upper}
import org.apache.spark.sql.{DataFrame, SparkSession}

object invoice_descrip_Q6 {

  def main(args:Array[String]): Unit ={

    val spark = SparkSession.builder()
      .appName("Invoice Description Parsing and Analysis")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    try{

      val data = List( ("I001", "Tshirt Red Large", 10, 20, 200),
        ("I002", "Jeans Blue Medium", 3, 50, 150),
        ("I003", "Jacket Black Small", 5, 100, 500),
        ("I004", "Jacket Black Small", 3, 100, 1001)
      )

      val df = spark.createDataFrame(data)
      val df1 = df.toDF("invoice_id","description","quantity","unit_price","total_amount")

      val product_name = regexp_extract(col("description"),"^([a-zA-z]+)",1)
      val color = regexp_extract(col("description"),"^([a-zA-z]+)\\s([a-zA-z]+)",2)
      val size = regexp_extract(col("description"),"([a-zA-z]+)$",1)

      def invoice(df2: DataFrame): DataFrame ={
        val result = df2.dropDuplicates("description")
          .select(col("*"),
            upper(product_name).alias("product_name"),
            color.alias("color"),
            size.alias("size"))
          .filter(!(col("quantity") < 5 && col("total_amount") > 1000))
          .groupBy(col("color"), col("size")).agg(
            sum(col("quantity")).alias("quantity"),
            avg(col("unit_price").alias("unit_price"))  )

        result
      }
      invoice(df1).show()
      df1.createOrReplaceTempView("sample")
      println("SQL LOGIC")
      spark.sql(
        """
        with dedup as (
        select invoice_id, description, quantity, unit_price, total_amount
        from sample
        group by invoice_id, description, quantity, unit_price, total_amount
        ),
        derived as (
        select invoice_id, description, quantity, unit_price, total_amount,
        upper(regexp_extract(description,"^([a-zA-z]+)",1)) as product_name,
        regexp_extract(description,"^([a-zA-z]+)\\s([a-zA-z]+)",2) as color,
        regexp_extract(description,"([a-zA-z]+)$",1) as size
        from dedup
        )
        select
        color,
        size,
        sum(quantity) as quantity,
        avg(unit_price) as unit_price
        from derived
        where !(quantity < 5 and total_amount > 1000)
        group by color, size
        """).show()

    } catch {
      case e: Exception => println(" Error Message : "+e.getMessage)
    } finally {
      spark.stop()
    }
  }

}
