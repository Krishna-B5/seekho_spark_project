package string_manipulation

import org.apache.spark.sql.functions.{col, concat, current_date, floor, initcap, lit, months_between, substring, to_date, upper, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

object CA_Q1 {

  def main(args:Array[String]): Unit ={

    val spark = SparkSession.builder()
      .appName("Combinational Assignment Question 1")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    try{

      val data = List(("karthik", "Data", 125000, "2018-05-10", Some(4.6)),
        ("pratik", "QA", 80000, "2020-03-15", Some(2.8)),
        ("veer", "Data", 60000, "2022-07-01", None),
        ("veena", "HR", 95000, "2019-01-20", Some(3.2)))

      val df = spark.createDataFrame(data)
      val df1 = df.toDF("name","dept","salary","last_promotion_date","performance_score")

      def promation(df2: DataFrame): DataFrame ={
        df2.select(col("name"),col("dept"),col("salary"),col("last_promotion_date"),col("performance_score"),
          concat(
          initcap(col("name")),
          lit("_"),
          upper(substring(col("dept"),0,2) ) ).alias("stand_emp_id"),
          floor(months_between(current_date(),to_date(col("last_promotion_date"))) / 12).alias("gap_years"),
          when(col("salary") * 12 >= 1500000,"HIGH_COST" )
            .when(col("salary") * 12 between( 900000, 1499999),"MID_COST")
            .otherwise("LOW_COST").alias("cost_level"),
          when(col("performance_score").isNull,"RISK_UNKNOWN")
            .when(floor(months_between(current_date(),to_date(col("last_promotion_date"))) / 12) > 4 and col("performance_score") < 3,"HIGH_RISK")
            .when(floor(months_between(current_date(),to_date(col("last_promotion_date"))) / 12) between( 2, 4),"MEDIUM_RISK")
            .otherwise("LOW_RISK").alias("risk_flag")
        )
      }
      promation(df1).show()

      df1.createOrReplaceTempView("sample")

      spark.sql(
        """select *,
          concat(initcap(name),
          "_",
          upper(substring(dept,0,2) ) ) as stand_emp_id,
          floor(months_between(current_date(),last_promotion_date) / 12) as gap_years,
          case
            when salary * 12 >= 1500000 then "HIGH_COST"
            when salary * 12 between 900000 and  1499999 then "MID_COST"
            else "LOW_COST"
          end as cost_level,
          case
            when performance_score != Null then "RISK_UNKNOWN"
            when floor(months_between(current_date(),last_promotion_date) / 12) > 4 and performance_score < 3 then "HIGH_RISK"
            when floor(months_between(current_date(),last_promotion_date) / 12) between 2 and 4 then "MEDIUM_RISK"
            else "LOW_RISK"
          end as risk_flag
          from sample """).show()

    } catch {
      case e: Exception => println("Error Message : "+e.getMessage)
    } finally {
      spark.stop()
    }
  }

}
