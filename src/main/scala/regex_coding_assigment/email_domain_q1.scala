package regex_coding_assigment

import org.apache.spark.sql.functions.{avg, col, count, datediff, regexp_extract, to_date, upper}
import org.apache.spark.sql.{DataFrame, SparkSession}

object email_domain_q1 {

  def main(args:Array[String]): Unit ={

    val spark = SparkSession.builder()
      .appName("Extration of email domain")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    try{

      val data = List(("U001","john.doe@gmail.com","2025-01-01","2025-03-01"),
        ("U002", "jane.smith@outlook.com", "2024-02-15", "2024-03-10"),
        ("U003", "alice.jones@yahoo.org", "2024-03-01", "2024-03-20"))

      val df = spark.createDataFrame(data)
      val df1 = df.toDF("user_id","email","signup_date","last_login")

      val domainExpr = regexp_extract(col("email"),"@([a-zA-Z0-9.]+)",1)
      val days = datediff(
        to_date(col("last_login")),
        to_date(col("signup_date"))
      )

      def domain1(df2: DataFrame): DataFrame ={
        val resultdf = df2.dropDuplicates("user_id")
          .select(col("*"),
            upper(domainExpr).alias("upper_domain")
             )
          .filter(!col("email").endsWith("org"))
          .groupBy("upper_domain")
          .agg(
            count("*").alias("cnt"),
            avg(days).alias("avg_days") )

        resultdf
      }

      domain1(df1).show(false)
      df1.createOrReplaceTempView("sample")
      println("SQL LOGIC")
      spark.sql(
        """
       WITH dedup AS (
        SELECT
        user_id,
        email,
        signup_date,
        last_login
    FROM sample
    GROUP BY user_id, email, signup_date, last_login
),
derived AS (
        SELECT
        user_id,email
        email,
        signup_date,
        last_login,

        upper(regexp_extract(email, '@([a-zA-Z0-9.]+)', 1)) AS upper_domain,
        datediff(
            to_date(last_login),
            to_date(signup_date)
        ) AS days_between
    FROM dedup
)
SELECT
    upper_domain,
    COUNT(*) AS user_count,
    AVG(days_between) AS avg_days_between_signup_login
FROM derived
WHERE upper_domain NOT LIKE '%.ORG'
GROUP BY upper_domain
ORDER BY user_count DESC """).show()

    } catch {
      case e: Exception => println("Error message : "+e.getMessage)
    } finally {
      spark.stop()
    }
  }

}
