package cryptics

import org.apache.spark.sql.SparkSession
import io.delta.tables.DeltaTable
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

object DataImport:

  private implicit val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .config("spark.sql.caseSensitive", value = true)
    .config("spark.sql.session.timeZone", value = "UTC")
    .config(
      "spark.sql.extensions",
      "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    .config(
      "spark.sql.catalog.spark_catalog",
      "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
    .appName("cryptics")
    .getOrCreate()

  def main(args: Array[String]): Unit =
    val tablePath: String        = "/tmp/cryptics/ticks"
    val dataSetPath: String      = "/data/datasets"
    val upsertPath: String       = dataSetPath + "/incremental.csv"
    val backFillPath: String     = dataSetPath + "/backfill.csv"
    val etlPaths: ETLPaths       = ETLPaths(tablePath, dataSetPath, upsertPath, backFillPath)
    val workload: LocalWorkloads = LocalWorkloads(etlPaths, spark)

    import LocalWorkloads._
    val dt: DeltaTable = workload.readTable()
    // workload.upsert()
    applyChanges(dt, Optimizations.VACUUM)
    // showTableInfo(dt)

  def loadTicks(): Unit =
    val df: Dataset[Row] = spark.read.csv("gs://datalabs-stage/cryptics/*.csv")
    import io.delta.tables._
    df.write.format("delta").save("gs://datalabs-stage/cryptics/ticks")
