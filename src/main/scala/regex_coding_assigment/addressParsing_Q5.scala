package regex_coding_assigment

import org.apache.spark.sql.functions.{col, countDistinct, min, regexp_extract, upper, max}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

object addressParsing_Q5 {

  def main(args:Array[String]): Unit ={

    val spark = SparkSession.builder()
      .appName("Address Parsing and Region Analaysis")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    try{

      val data = Seq(("A001","123 Main St Apt 4B","New York","Ny","10000"),
        ("A002","456 Elm St Apt 5C","San Francisco","Ca","9410"),
        ("A003","789 Oak St Apt 12C","Chicago","iL","60603"),
        ("A004","456 Elm St Apt 5C","San Francisco","CA","9410"),
        ("A005","56 Elm St Apt 5C","San Francisco","Ca","9410") )

      val df = spark.createDataFrame(data)
      val df1 = df.toDF("address_id","full_add","city","state","zipcode")

      val street_num = regexp_extract(col("full_add"),"(^[0-9]+)",1)
      val street_name = regexp_extract(col("full_add"),"(^[0-9]+)\\s([A-Za-z]+\\s+[a-zA-Z]+)",2)
      val apartment_num = regexp_extract(col("full_add"),"(A[a-zA-Z\\s0-9a-zA-Z]+)$",1)

      def addPrasing(df2: DataFrame): DataFrame ={
        val resultdf = df2.dropDuplicates("full_add")
          .select(col("*"),
            upper(col("state")).alias("state_upper"),
            street_num.alias("street_num"),
            street_name.alias("street_name"),
            apartment_num.alias("apartment_num")  )
          .filter(!(col("city").startsWith("New") && col("zipcode").endsWith("00")))
          .groupBy(col("state_upper")).agg(
            countDistinct(col("city")).alias("city_count"),
            min(col("street_num")).alias("Min_Street"),
            max(col("street_num")).alias("Max_Street")
          )
        resultdf
      }

      addPrasing(df1).show()
      df1.createOrReplaceTempView("sample")
      spark.sql(
        """
          with dedup as(
          select address_id, full_add, city, state,zipcode
          from sample
          group by address_id, full_add, city, state,zipcode
          ),
          derived as (
          select address_id, full_add, city, state,zipcode,
          upper(state) as state_upper,
          regexp_extract(full_add,"(^[0-9]+)",1) as street_num,
          regexp_extract(full_add,"(^[0-9]+)\\s([A-Za-z]+\\s+[a-zA-Z]+)",2) as street_name,
          regexp_extract(full_add,"(A[a-zA-Z\\s0-9a-zA-Z]+)$",1) as apartment_num
          from dedup
          )
          select
          state_upper,
          count(distinct city) as city_count,
          min(street_num) as Min_Street,
          max(street_num) as Max_Street
          from derived
          where !(city like '%New' and zipcode like '10000')
          group by state_upper
          """).show()

    }catch {
      case e: Exception => println(" Error Message : "+e.getMessage)
    } finally {
      spark.stop()
    }


  }

}
