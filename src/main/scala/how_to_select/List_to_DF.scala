package how_to_select

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object List_to_DF {

  def main(args:Array[String]):Unit={

    val spark = SparkSession.builder()        //creating a sparksession
      .appName("List to DF")
      .master("local[*]")
      .getOrCreate()

    // we can going to create a list and converting to DataFrame
    val mylist = List(
      (1,"krishna",36,4500),
      (1,"Kiran",35,5000),
      (1,"Veer",26,3500),
      (1,"Jai",46,6500)
    )

    val df = spark.createDataFrame(mylist)

    val df1 = df.toDF("Id","Name","Age","Salary")

    df1.show()

    val df2= df1.select(
      col("Id"),
      col("Name"),
      col("Age"),
      when(col("Age") > 35,"Eligible")
        .otherwise("Not-Eligible").alias("Status")
    ).show()
  }

}
