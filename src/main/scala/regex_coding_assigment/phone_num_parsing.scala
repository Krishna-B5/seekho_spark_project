package regex_coding_assigment

import org.apache.spark.sql.functions.{col, count, lower, regexp_extract, regexp_replace}
import org.apache.spark.sql.{DataFrame, SparkSession}

object phone_num_parsing {

  def main(args:Array[String]): Unit ={

    val spark = SparkSession.builder()
      .appName("Phone Number Parsing")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    try{

      val data = Seq(("C001", "+1-415-5551234"," 2025-01-10","WEST"),
        ("C002","+44-20-79461234","2025-02-20","EAST"),
        ("C003","+91-22-23451234","2025-03-15","NORTH"),
        ("C004", "+1-415-23451234"," 2025-02-10","WEST"))

      val df = spark.createDataFrame(data)
      val df1 = df.toDF("cust_id","phone_num","signup_date","region")

      val countryCode = regexp_extract(col("phone_num"),"([+0-9]+)",1)
      val areaCode = regexp_extract(col("phone_num"),"([+0-9]+)\\-([0-9]+)",2)
      val localNumber = regexp_extract(col("phone_num"),"([0-9]+)$",1)

      def parsing(df2: DataFrame): DataFrame ={
        val resultdf = df2.dropDuplicates("phone_num")
          .select(col("*"),
            lower(col("region")).alias("low_region"),
            countryCode.alias("country_code"),
            areaCode.alias("area_code"),
            localNumber.alias("local_number") )
          .filter(col("country_code") notEqual  "+1")
          .groupBy("low_region")
          .agg(count("cust_id").alias("counts"),
            count("area_code").alias("area_freq"))

        resultdf
      }

      parsing(df1).show()
      df1.createOrReplaceTempView("sample")
      println("SQL Logic")
      spark.sql(
        """
          with dedup as (
          select cust_id, phone_num, signup_date, region
          from sample
          group by cust_id, phone_num, signup_date, region
          ),
          derived as(
          select
          cust_id,
          phone_num,
          signup_date,
          lower(region) as low_region,
          regexp_extract(phone_num,"([+0-9]+)",1) as countryCode,
          regexp_extract(phone_num,"([+0-9]+)\\-([0-9]+)",2) as areaCode,
          regexp_extract(phone_num,"([0-9]+)$",1) as localNumber
          from dedup
          )
          select
          low_region,
          count("cust_id") as counts,
          count("area_code") as area_freq
          from derived
          WHERE !(countrycode =  "+1")
          group by low_region
          """).show()

    } catch {
      case e: Exception => println(" Error Message : "+e.getMessage)
    } finally {
      spark.stop()
    }
  }
}
