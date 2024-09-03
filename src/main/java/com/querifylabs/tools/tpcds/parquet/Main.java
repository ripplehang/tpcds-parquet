package com.querifylabs.tools.tpcds.parquet;

import org.apache.spark.sql.SparkSession;
import org.apache.commons.cli.*;
import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class Main {

  public static class AppConfig {
    String accessKey;
    String secretKey;
    String s3bucket;
    String hiveMetaUrl;
    String datasetSize;
    String sqlpath;
    String schemaPath;
    boolean needCreateSchema;

    public AppConfig(String accessKey, String secretKey, String s3bucket, String hiveMetaUrl,
      String datasetSize, String sqlpath, String schemaPath, boolean needCreateSchema) {
      this.accessKey = accessKey;
      this.secretKey = secretKey;
      this.s3bucket = s3bucket;
      this.hiveMetaUrl = hiveMetaUrl;
      this.datasetSize = datasetSize;
      this.sqlpath = sqlpath;
      this.schemaPath = schemaPath;
      this.needCreateSchema = needCreateSchema;
    }
  }

  public static AppConfig readConfig(String configFilePath) throws IOException {
    Properties props = new Properties();
    try (FileInputStream fis = new FileInputStream(configFilePath)) {
      props.load(fis);
    }

    boolean needCreateSchema = "true".equals(props.getProperty("need_create_schema"));

    return new AppConfig(
      props.getProperty("access_key"),
      props.getProperty("secret_key"),
      props.getProperty("s3bucket"),
      props.getProperty("hive_meta_url"),
      props.getProperty("datasetsize"),
      props.getProperty("sql_path"),
      props.getProperty("schema_path"),
      needCreateSchema
    );
  }

  public static class CommandLineConfig {
    String configFile = "";

    public CommandLineConfig(String configFile) {
      this.configFile = configFile;
    }
  }

  public static Optional<AppConfig> parseArguments(String[] args) {
    Options options = new Options();
    Option configOption = new Option("c", "config", true, "Path to configuration file");
    configOption.setRequired(true);
    options.addOption(configOption);

    CommandLineParser parser = new DefaultParser();
    HelpFormatter formatter = new HelpFormatter();

    try {
      CommandLine cmd = parser.parse(options, args);
      String configFile = cmd.getOptionValue("config");
      return Optional.of(readConfig(configFile));
    } catch (ParseException e) {
      System.out.println("Parsing failed. Reason: " + e.getMessage());
      formatter.printHelp("SparkSQLExecutor", options);
      return Optional.empty();
    } catch (IOException e) {
      e.printStackTrace();
      return Optional.empty();
    }
  }

  public static void createMetadata(SparkSession spark, String s3bucket, String schemaPath, String datasetsize) throws IOException {
    Path resourcesDir = Paths.get(schemaPath);

    try (Stream<Path> paths = Files.list(resourcesDir)) {
      List<Path> schemaFiles = paths
        .filter(p -> p.toString().endsWith(".schema"))
        .collect(Collectors.toList());

      String databaseName = "tpcds_" + datasetsize;
      String createDatabaseStatement = String.format(
        "CREATE DATABASE IF NOT EXISTS %s LOCATION 's3a://%s/tpcds/%s'",
        databaseName, s3bucket, datasetsize
      );
      spark.sql(createDatabaseStatement);

      for (Path schemaFilePath : schemaFiles) {
        String schemaContent = new String(Files.readAllBytes(schemaFilePath));
        String tableName = schemaFilePath.getFileName().toString().replace(".schema", "");
        String createTableStatement = String.format(
          "CREATE EXTERNAL TABLE IF NOT EXISTS %s.%s (%s) STORED AS PARQUET LOCATION 's3a://%s/tpcds/%s/%s'",
          databaseName, tableName, schemaContent, s3bucket, datasetsize, tableName
        );
        spark.sql(createTableStatement);
        System.out.println("=======>: " + tableName + " is created");
      }
    }
  }

  public static void logToCSV(String fileName, double duration, long resultCount, boolean exceptionOccurred, String csvFilePath) throws IOException {
    String exceptionMarker = exceptionOccurred ? "EXCEPTION" : "";
    String logLine = String.format("%s,%f,%d,%s%n", fileName, duration, resultCount, exceptionMarker);
    Path path = Paths.get(csvFilePath);

    synchronized (Main.class) {
      if (!Files.exists(path)) {
        Files.write(path, "FileName,Duration(ms),ResultCount,Exception\n".getBytes(), StandardOpenOption.CREATE);
      }
      Files.write(path, logLine.getBytes(), StandardOpenOption.APPEND);
    }
  }

  public static void execSQL(SparkSession spark, String sql, String fileName, String sqlPath, String datasize) {
    long start = System.nanoTime();
    long resultCount = 0;
    boolean exceptionOccurred = false;
    try {
      var df = spark.sql(sql);
      resultCount = df.count();
      System.out.println("Successfully executed SQL from file: " + fileName + ", Result count: " + resultCount);
    } catch (Exception e) {
      exceptionOccurred = true;
      System.out.println("Error executing SQL from file: " + fileName + " - " + e.getMessage());
    }
    long end = System.nanoTime();
    double duration = (end - start) / 1e6; // Convert nanoseconds to milliseconds
    System.out.println("Execution time for file " + fileName + ": " + duration + " ms");

    try {
      logToCSV(fileName, duration, resultCount, exceptionOccurred, Paths.get(sqlPath, "tpcds_perf_" + datasize + ".csv").toString());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void executeAllSQL(SparkSession spark, String sqlPath, String datasize) throws IOException {
    String databaseName = "tpcds_" + datasize;
    spark.sql(String.format("USE %s", databaseName));
    Path sqlsDir = Paths.get(sqlPath);

    try (Stream<Path> paths = Files.list(sqlsDir)) {
      List<Path> sqlFiles = paths
        .filter(p -> p.toString().endsWith(".sql"))
        .collect(Collectors.toList());

      for (Path sqlFilePath : sqlFiles) {
        String sqlStatement = new String(Files.readAllBytes(sqlFilePath));
        execSQL(spark, sqlStatement, sqlFilePath.getFileName().toString(), sqlPath, datasize);
      }
    }
  }

  public static SparkSession initSparkSession(AppConfig appConfig) {
    return SparkSession.builder()
      .appName("Load DataFrame from URL")
      .master("local[*]")
      .config("spark.sql.catalogImplementation", "hive")
      .config("spark.hadoop.fs.s3a.access.key", appConfig.accessKey)
      .config("spark.hadoop.fs.s3a.secret.key", appConfig.secretKey)
      .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
      .config("fs.s3a.bucket." + appConfig.s3bucket + ".access.key", appConfig.accessKey)
      .config("fs.s3a.bucket." + appConfig.s3bucket + ".secret.key", appConfig.secretKey)
      .config("hive.metastore.uris", "thrift://" + appConfig.hiveMetaUrl)
      .config("spark.executorEnv.LD_LIBRARY_PATH", "")
      .config("spark.sql.legacy.charVarcharAsString", "true")
      .config("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")
      .enableHiveSupport()
      .getOrCreate();
  }

  public static void main(String[] args) {
    Logger.getRootLogger().setLevel(Level.WARN);
    Optional<AppConfig> appConfigOpt = parseArguments(args);
    if (appConfigOpt.isPresent()) {
      try {
        AppConfig appConfig = appConfigOpt.get();
        SparkSession spark = initSparkSession(appConfig);
        if (appConfig.needCreateSchema) {
          createMetadata(spark, appConfig.s3bucket, appConfig.schemaPath, appConfig.datasetSize);
        }
        executeAllSQL(spark, appConfig.sqlpath, appConfig.datasetSize);
        spark.stop();
      } catch (IOException e) {
        e.printStackTrace();
        System.exit(1);
      }
    } else {
      System.exit(1);
    }
  }
}
