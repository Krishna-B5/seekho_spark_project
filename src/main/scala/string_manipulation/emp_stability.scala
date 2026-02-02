package string_manipulation

import org.apache.spark.sql.functions.{col, concat, current_date, floor, lit, months_between, round, substring, to_date, upper, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

object emp_stability {

  def main(args:Array[String]): Unit ={

    val spark = SparkSession.builder()
      .appName(" Employee Stability and Attrition ")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    try{

      val data = List(("karthik", "Bangalore", 130000, "2016-06-01", 22),
        ("pratik", "Pune", 85000, "2021-01-10", 18),
        ("veer", "Delhi", 60000, "2023-02-15", 5),
        ("veena", "Chennai", 95000, "2019-09-20", 10)
      )

      val df = spark.createDataFrame(data)
      val df1 = df.toDF("name","location","salary","join_date","leaves")

      def stability(df2: DataFrame): DataFrame ={

        val tenuremonths = floor(months_between(current_date(),to_date(col("join_date"))))
        val leaveIntesity = round((col("leaves") / tenuremonths) * 12 ,0)

        df2.select(col("*"),
          concat(
          upper(col("name")),
          lit("-"),
          substring(col("location"),1,3) ).alias("emp_tag"),
          tenuremonths.alias("tenure_months"),
          round((col("leaves") / tenuremonths) * 12 ,0).alias("leave_intesity"),
          when(tenuremonths < 24 && leaveIntesity > 15,"VERY_HIGH")
            .when(tenuremonths.between(24, 48),"MEDIUM")
            .when(tenuremonths > 24 && leaveIntesity < 5,"LOW")
            .otherwise("MODERATE").alias("attrition") )
      }
      stability(df1).show()

      df1.createOrReplaceTempView("sample")
      println(" SQL LOGIC ")
      spark.sql(
        """ select *,
          concat(
          upper(name),
          "-",
          substring(location,1,3) ) as emp_tag,
          floor(months_between(current_date(),to_date(join_date))) as tenure_months,
          round((leaves / floor(months_between(current_date(),to_date(join_date)))) * 12 ,0) as leave_intensity,
          case
            when floor(months_between(current_date(),to_date(join_date))) < 24 and round((leaves / floor(months_between(current_date(),to_date(join_date)))) * 12 ,0) > 15 then "VERY_HIGH"
            when floor(months_between(current_date(),to_date(join_date))) between 24 and 48 then "MEDIUM"
            when floor(months_between(current_date(),to_date(join_date))) > 48 and round((leaves / floor(months_between(current_date(),to_date(join_date)))) * 12 ,0) < 5 then "LOW"
            else "MODERATE"
          end as attrition
          from sample""").show()


    } catch {
      case e:Exception => println("Error Message : "+e.getMessage)
    } finally {
      spark.stop()
    }
  }

}
