package regex_coding_assigment

import org.apache.spark.sql.functions.{col, countDistinct, initcap, regexp_extract, sum}
import org.apache.spark.sql.{DataFrame, SparkSession}

object productCodeAnalysis_Q3 {

  def main(args:Array[String]): Unit ={

    val spark = SparkSession.builder()
      .appName("Product code Analysis")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    try{

      val data = List(("P001", "A123-456-789", "2025-04-01", "electronics"),
        ("P002", "B234-567-891", "2025-05-10","home_appliance"),
        ("P003", "X345-678-999", "2025-06-15", "furniture"),
        ("P004", "X345-678-989", "2025-07-15", "furniture"),
        ("P005", "X345-668-999", "2025-09-15", "furniture"),
        ("P006", "X345-688-999", "2025-08-15", "home_appliance"))

      val df = spark.createDataFrame(data)
      val df1 = df.toDF("product_id","product_code","release_date","category")

      val brandCode = regexp_extract(col("product_code"),"^([a-zA-Z0-9]+)",1)
      val categoryCode = regexp_extract(col("product_code"),"^([a-zA-Z0-9]+)\\-([0-9]+)",2)
      val serialNum = regexp_extract(col("product_code"),"([0-9]+)$",1)

      def analysis(df2: DataFrame): DataFrame ={
        val resultdf = df2.dropDuplicates("product_code")
          .select(col("*"),
            initcap(col("category")).alias("category_init"),
            brandCode.alias("brand_code"),
            categoryCode.alias("categroy_code"),
            serialNum.alias("serial_number")  )
          .filter(col("brand_code").contains("X") && col("serial_number").endsWith("99")).alias("filtered_record")
          .groupBy(col("category_init")).agg(sum(col("serial_number")).alias("ttl_serial_num"),countDistinct(col("brand_code")).alias("distinct_brand_code"))

        resultdf
      }

      analysis(df1).show()
      df1.createOrReplaceTempView("sample")
      println("SQL LOGIC")
      spark.sql(
        """
          with dedup as (
          select product_id, product_code, release_date, category
          from sample
          group by product_id, product_code, release_date, category
          ),
          derived as (
          select product_id, product_code, release_date,
          upper(category) as category_init,
          regexp_extract(product_code,"^([a-zA-Z0-9]+)",1) as brand_code,
          regexp_extract(product_code,"^([a-zA-Z0-9]+)\\-([0-9]+)",2) as categroy_code,
          regexp_extract(product_code,"([0-9]+)$",1) as serial_number
          from dedup
          )
          select
          category_init,
          sum(serial_number) as ttl_serial_num,
          count(distinct "brand_code") as distinct_brand_code
          from derived
          where !(brand_code not like '%X' and serial_number not like '%99')
          group by category_init
          """).show()

    } catch {
      case e: Exception => println("Error Message")
    } finally {
      spark.stop()
    }
  }

}
