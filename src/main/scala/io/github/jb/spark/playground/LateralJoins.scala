package io.github.jb.spark.playground

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object LateralJoins {

  def getDepartmentDF(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    Seq(
      (1, "Engineering", 100000),
      (2, "Sales", 80000),
      (3, "Marketing", 90000)
    ).toDF("id", "dept_name", "budget")
  }

  def getEmployeeDF(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    Seq(
      (1, "John", "Engineering", 75000),
      (1, "Jane", "Engineering", 85000),
      (2, "Mike", "Sales", 60000),
      (2, "Sarah", "Sales", 65000),
      (3, "Tom", "Marketing", 70000),
      (3, "Ann", "Marketing", 64000)
    ).toDF("dept_id", "name", "department", "salary")
  }

  def main(args: Array[String]): Unit = {

    implicit val spark: SparkSession =
      SparkSession.builder().appName("LateralJoinExample").master("local[*]").getOrCreate()

    import spark.implicits._

    {
      // let calculate employees having the smallest salary per department as inner join
      // we must use window function to calculate the rank, then restrict ds to only 1 row per department
      val departments = getDepartmentDF
      val employees   = getEmployeeDF

      val rankedEmployees = employees
        .withColumn("rank", row_number().over(Window.partitionBy("department").orderBy($"salary".asc)))
        .where($"rank" <= 1)

      val result = departments
        .join(rankedEmployees, departments("id") === rankedEmployees("dept_id"))
        .select("id", "dept_name", "budget", "name", "department", "salary")

      println("Inner join result:")
      result.show()
    }

    {
      // let calculate employees having the smallest salary per department as lateral join
      val departments = getDepartmentDF.alias("ds")
      val employees   = getEmployeeDF.alias("es")

      val result = departments
        .lateralJoin(
          employees
            .where(col("ds.id").outer === col("es.dept_id"))
            .orderBy($"salary".asc)
            .limit(1)
        )
        .select("id", "dept_name", "budget", "name", "department", "salary")

      println("Lateral join result:")
      result.show()
    }

    {
      // let calculate employees having the smallest salary per department as lateral join with SQL API
      getDepartmentDF.createTempView("ds")
      getEmployeeDF.createTempView("es")

      val result = spark
        .sql(
          """
          |SELECT ds.id, ds.dept_name, ds.budget, es.name, es.department, es.salary FROM ds,
          |LATERAL (
          |  SELECT *
          |  FROM es
          |  WHERE ds.id = es.dept_id
          |  ORDER BY salary ASC
          |  LIMIT 1
          |) AS es
          |""".stripMargin
        )

      println("Lateral join result as SQL:")
      result.show()
    }

    spark.close()
  }
}
