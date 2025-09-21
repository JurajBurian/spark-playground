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
    ).toDF("id", "department", "budget")
  }

  def getEmployeeDF(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    Seq(
      (1, "John", 75000),
      (1, "Jane", 85000),
      (2, "Mike", 60000),
      (2, "Sarah", 65000),
      (3, "Tom", 70000),
      (3, "Ann", 64000)
    ).toDF("dept_id", "name", "salary")
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
        .withColumn("rank", row_number().over(Window.partitionBy("dept_id").orderBy($"salary".asc)))
        .where($"rank" <= 1)

      val result = departments
        .join(rankedEmployees, departments("id") === rankedEmployees("dept_id"))
        .select("id", "budget", "name", "department", "salary")

      println("Inner join result:")
      result.show()
    }

    {
      // let calculate employees having the smallest salary per department as lateral join
      val departments = getDepartmentDF.alias("ds").repartition(2, $"id")
      val employees   = getEmployeeDF.alias("es").repartition(2, $"dept_id")

      val result = departments
        .lateralJoin(
          employees
            .where(col("ds.id").outer === col("es.dept_id"))
            .orderBy($"salary".asc)
            .limit(1)
        )
        .select("id", "budget", "name", "department", "salary")

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
          |SELECT ds.id, ds.budget, es.name, ds.department, es.salary
          |FROM ds
          |INNER JOIN LATERAL (
          |  SELECT *
          |  FROM es
          |  WHERE ds.id = es.dept_id
          |  ORDER BY salary ASC
          |  LIMIT 1
          |) AS es""".stripMargin
        )

      println("Lateral join result as SQL:")
      result.show()
    }

    spark.close()
  }
}
