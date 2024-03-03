package cryptics

import org.apache.spark.sql.SparkSession
import io.delta.tables.DeltaTable
import org.apache.spark.sql.Row
import org.apache.spark.sql.Dataset

enum Optimizations:
  case COMPACT extends Optimizations
  case VACUUM  extends Optimizations
  def handleVacuum(): String = this.toString()

case class ETLPaths(tablePath: String, datasetPath: String, upsertPath: String, backFillPath: String)

class LocalWorkloads(etlPaths: ETLPaths, implicit val spark: SparkSession):
  import LocalWorkloads._
  import etlPaths._

  private val loadCSV: (String, SparkSession) => Dataset[Row] = (path: String, spark: SparkSession) => spark.read.option("header", "true").csv(path)

  def backFill(): Unit =
    val backFillDf: Dataset[Row] = loadCSV(backFillPath, spark)
    backFillDf.write.format("delta").save(tablePath)

  def readTable(): DeltaTable =
    import io.delta.tables._
    DeltaTable.forPath(tablePath)

  def upsert(upsertExpr: String = "target.Id = source.Id"): Unit =
    val table: DeltaTable       = readTable()
    val dataFrame: Dataset[Row] = loadCSV(upsertPath, spark)
    mergeTables(table, dataFrame, upsertExpr)

object LocalWorkloads:
  import Optimizations._
  def apply(etlPaths: ETLPaths, spark: SparkSession): LocalWorkloads = new LocalWorkloads(etlPaths, spark)

  def mergeTables(table: DeltaTable, dataFrame: Dataset[Row], upsertExpr: String): Unit =
    table
      .as("target")
      .merge(dataFrame.as("source"), upsertExpr)
      .whenMatched()
      .updateAll()
      .whenNotMatched()
      .insertAll()
      .execute()

  def applyChanges(deltaTable: DeltaTable, operation: Optimizations = COMPACT): Unit =
    operation match
      case COMPACT => deltaTable.optimize().executeCompaction()
      case VACUUM  => deltaTable.vacuum(10)

  def showTableInfo(deltaTable: DeltaTable): Unit = deltaTable.history().show(false)
