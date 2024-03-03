package cryptics

import munit.Location
import cryptics.LocalTestSuite
import cryptics.SparkSessionTestWrapper
import com.github.mrpowers.spark.fast.tests.DatasetComparer
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

class LocalTestSuite extends munit.FunSuite with SparkSessionTestWrapper with DatasetComparer:
  implicit val loc: Location = munit.Location("", 1)

class DataImportTestSpec extends LocalTestSuite:
  import Optimizations._
  import spark.implicits._

  test("for asserting vacuum optimize") {
    val sourceDF = Seq(
      ("jose"),
      ("li"),
      ("luisa")
    ).toDF("name")
    val actualDF = sourceDF.select(col("name").alias("student"))
    val expectedDF = Seq(
      ("jose"),
      ("li"),
      ("luisa")
    ).toDF("student")
    assertSmallDatasetEquality(actualDF, expectedDF)
  }
