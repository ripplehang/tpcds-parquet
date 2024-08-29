import org.apache.spark.sql.SparkSession

import scala.io.Source
import java.nio.file.{Files, Paths, StandardOpenOption}
import scala.collection.JavaConverters._
import scala.util.control.NonFatal
import java.util.Properties
import java.io.FileInputStream
import scopt.OParser


object MainTest {
  case class AppConfig(
                        accessKey: String,
                        secretKey: String,
                        s3bucket: String,
                        hiveMetaUrl: String,
                        datasetSize: String,
                        sqlpath: String,
                        schemaPath:String,
                        needCreateSchema:Boolean
                      )

  // Function to read the configuration properties from a file
  def readConfig(configFilePath: String): AppConfig = {
    val props = new Properties()
    props.load(new FileInputStream(configFilePath))
    val needCreateSchema = props.getProperty("need_create_schema") match {
      case "true" => true
      case _ => false
    }

    AppConfig(
      accessKey = props.getProperty("access_key"),
      secretKey = props.getProperty("secret_key"),
      s3bucket = props.getProperty("s3bucket"),
      hiveMetaUrl = props.getProperty("hive_meta_url"),
      datasetSize = props.getProperty("datasetsize"),
      sqlpath = props.getProperty("sql_path"),
      schemaPath = props.getProperty("schema_path"),
      needCreateSchema = needCreateSchema
    )
  }

  // Define a case class to hold the command-line options
  case class CommandLineConfig(configFile: String = "")

  // Function to parse command-line arguments and return AppConfig
  def parseArguments(args: Array[String]): Option[AppConfig] = {
    val builder = OParser.builder[CommandLineConfig]
    val parser = {
      import builder._
      OParser.sequence(
        programName("SparkSQLExecutor"),
        head("SparkSQLExecutor", "1.0"),
        opt[String]('c', "config")
          .required()
          .valueName("<file>")
          .action((x, c) => c.copy(configFile = x))
          .text("Path to configuration file")
      )
    }

    // Parse the command-line arguments
    OParser.parse(parser, args, CommandLineConfig()) match {
      case Some(cliConfig) =>
        // Read the configuration from the specified file
        Some(readConfig(cliConfig.configFile))
      case _ =>
        // Arguments are bad, error message will have been displayed
        None
    }
  }

  def create_metadata(spark:SparkSession, s3bucket:String,schemaPath:String,datasetsize:String):Unit = {
        // Path to the resources directory
        val resourcesDir = Paths.get(schemaPath)

        // List all SQL files in the resources directory
        val schemaFiles = Files.list(resourcesDir)
          .iterator()
          .asScala
          .filter(_.toString.endsWith(".schema"))
          .toList

        // Define the database name
        val databaseName = "tpcds_" + datasetsize

        val createDatabaseStatement = s"""create database if not exists $databaseName LOCATION 's3a://$s3bucket/tpcds/$datasetsize' """
        spark.sql(createDatabaseStatement)

        // Iterate over each schema file
        schemaFiles.foreach { schemaFilePath =>
          // Read the schema file content
          val schemaContent = Source.fromFile(schemaFilePath.toFile).getLines.mkString(" ")

          // Extract the table name from the file name (assuming the file name is the table name)
          val tableName = schemaFilePath.getFileName.toString.replace(".schema", "")


          // Construct the CREATE TABLE statement
          val createTableStatement =
            s"""
               |CREATE EXTERNAL TABLE IF NOT EXISTS $databaseName.$tableName(
               |$schemaContent
               |) STORED AS PARQUET LOCATION 's3a://$s3bucket/tpcds/$datasetsize/$tableName'
        """.stripMargin

          // Execute the CREATE TABLE statement
          spark.sql(createTableStatement)
          print("=======>: "+ tableName + " is created\n")

          // Optionally, show the first few rows of the newly created table
          //spark.sql(s"SELECT * FROM $databaseName.$tableName limit 1").show(truncate = false)
        }
  }
  // Define a function to log the execution time and filename to a CSV file
  // Define a function to log the execution time, filename, and result count or exception to a CSV file
  def logToCSV(fileName: String, duration: Double, resultCount: Long, exceptionOccurred: Boolean, csvFilePath: String): Unit = {
    val exceptionMarker = if (exceptionOccurred) "EXCEPTION" else ""
    val logLine = s"$fileName,$duration,$resultCount,$exceptionMarker\n"
    val path = Paths.get(csvFilePath)
    this.synchronized {
      if (!Files.exists(path)) {
        Files.write(path, "FileName,Duration(ms),ResultCount,Exception\n".getBytes, StandardOpenOption.CREATE)
      }
      Files.write(path, logLine.getBytes, StandardOpenOption.APPEND)
    }
  }



  def execSQL(spark:SparkSession,sql:String, fileName:String,sqlPath:String,datasize:String): Unit = {
    val start = System.nanoTime()
    var resultCount = 0L
    var exceptionOccurred = false
    try {
      val df = spark.sql(sql) // Execute the SQL statement
      resultCount = df.count() // Get the number of result rows
      println(s"Successfully executed SQL from file: $fileName, Result count: $resultCount")
    } catch {
      case NonFatal(e) =>
        exceptionOccurred = true
        println(s"Error executing SQL from file: $fileName - ${e.getMessage}")
    }
    val end = System.nanoTime()
    val duration = (end - start) / 1e6d // Convert nanoseconds to milliseconds
    println(s"Execution time for file $fileName: $duration ms")

    // Log the execution time, filename, result count, and exception marker to a CSV file
    logToCSV(fileName, duration, resultCount, exceptionOccurred, s"$sqlPath\\tpcds_perf_$datasize.csv")

  }


  def ExecuteAllSQL(spark:SparkSession,sqlPath:String,datasize:String):Unit = {
    val sqlsDir = Paths.get(sqlPath)

    // List all SQL files in the resources directory
    val sqlFiles = Files.list(sqlsDir)
      .iterator()
      .asScala
      .filter(_.toString.endsWith(".sql"))
      .toList

    // Iterate over each schema file
    sqlFiles.foreach { sqlFilePath =>
      // Read the schema file content
      val fileSource = Source.fromFile(sqlFilePath.toFile)
      val sqlStatement = try {
        fileSource.mkString // This preserves the original file formatting
      } finally {
        fileSource.close() // Make sure to close the file after reading
      }

      // Execute the CREATE TABLE statement
      execSQL(spark,sqlStatement,sqlFilePath.getFileName.toString(),sqlPath,datasize)
    }
  }

  def InitSparkSession(appConfig: AppConfig):SparkSession = {
    // Create SparkSession
    val spark = SparkSession.builder()
      .appName("Load DataFrame from URL")
      .master("local[*]") // Run in local mode using all available cores
      .config("spark.sql.catalogImplementation", "hive")
      .config("spark.hadoop.fs.s3a.access.key", appConfig.accessKey)
      .config("spark.hadoop.fs.s3a.secret.key", appConfig.secretKey)
      .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
      .config(s"fs.s3a.bucket.${appConfig.s3bucket}.access.key", appConfig.accessKey)
      .config(s"fs.s3a.bucket.${appConfig.s3bucket}.secret.key", appConfig.secretKey)
      .config(s"hive.metastore.uris", s"thrift://${appConfig.hiveMetaUrl}")
      .config("spark.executorEnv.LD_LIBRARY_PATH", "")
      .config("spark.sql.legacy.charVarcharAsString", "true")
      .config("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.sql(s"USE tpcds_${appConfig.datasetSize}")
    spark
  }
  def main(args: Array[String]): Unit = {
    parseArguments(args) match {
      case Some(appConfig) =>
        val spark = InitSparkSession(appConfig)
        if(appConfig.needCreateSchema){
          create_metadata(spark,appConfig.s3bucket,appConfig.schemaPath,appConfig.datasetSize)
        }
        ExecuteAllSQL(spark,appConfig.sqlpath,appConfig.datasetSize)
        spark.stop()
      case None =>
        // Arguments are bad, error message will have been displayed
        System.exit(1)
    }
  }
}