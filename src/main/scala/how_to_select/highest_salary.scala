package how_to_select

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions._

object highest_salary {

  def main(args:Array[String]): Unit ={

    val spark = SparkSession.builder()
      .appName("Highest Salary")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val data = List((1,"Ravi","IT",85000), (2,"Krishna","IT",95000), (3,"Priya","HR",65000),
      (4,"Neha","HR",70000), (5,"Kiran","Mech",50000) )

    val df = spark.createDataFrame(data)
    val df1 = df.toDF("id","name","dept","salary")



    val df2 = df1
      .groupBy(col("dept"))
      .agg(max("salary").alias("max_salary"))
      .orderBy(desc("max_salary"))

    df2.show()


  }

}
