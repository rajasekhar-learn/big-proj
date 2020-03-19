package com.mdb.sample;


import com.mdb.sample.utils.HDFSUtil;
import com.mdb.sample.utils.HttpUtil;
import com.mdb.sample.utils.PropertiesUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import static com.mdb.sample.constants.ModuleConstants.*;

/**
 * This class is driver . which creates RDD and generate logical and physical planes and collect results from executors.
 */
public class BatchDataAnalyser {

    private static final Logger LOGGER = LoggerFactory.getLogger(BatchDataAnalyser.class);
    /**
     * Boot strapping point. expects list of argument, that program can expect to run.
     *
     * @param args expects 1 URL argument(arg[0]), from where data can be fetched.
     */
    public static void main(String[] args) {
        try {
            //hive warehouse location
            String warehouseLocation = PropertiesUtils.getPropertyValue(HIVE_WAREHOUSE_LOCATION);

            SparkSession spark = SparkSession
                    .builder()
                    .master("local[*]")
                    .appName("Spark app")
                    .config("spark.sql.warehouse.dir", warehouseLocation)
                    .enableHiveSupport()
                    .getOrCreate();

            String url = args.length > 0 ? args[0] : "";
            if (StringUtils.isNotEmpty(url)) {
                //load csv from URL to HDFS.
                String fileLocation=PropertiesUtils.getPropertyValue(FILE_DOWNLOAD_LOCATION)+System.currentTimeMillis()+"-data.csv";
                HttpUtil.downloadFile(fileLocation,url);
                HDFSUtil.createHdfsDir(PropertiesUtils.getPropertyValue(HADOOP_FILE_COPY_PATH));
                HDFSUtil.copyFileToHDFS(fileLocation,PropertiesUtils.getPropertyValue(HADOOP_FILE_COPY_PATH));
                String hdfsFilePath=PropertiesUtils.getPropertyValue(HADOOP_FILE_COPY_PATH)+fileLocation.substring(fileLocation.lastIndexOf(File.separator)+1,fileLocation.length());
                LOGGER.debug("File location in HDFS :: {} ",hdfsFilePath);
                //dataset creation from HDFS path
                Dataset<Row> data = spark.read().option("header", "true")
                        .option("inferSchema", "true")
                        .csv(hdfsFilePath);
                //given data set column name has issues(like space added to High_Confidence_Limit), due to this only initial load works,next appends fail. to support appending
                //data to hive table in next runs, i am converting raw inferred dataset to hive supported one, by trimming and converting columns to lower case.
                Dataset<Row> hiveFormattedDataSet = data.toDF(Arrays.asList(data.columns()).stream().map(x -> x.toLowerCase().trim()).toArray(size -> new String[size]));
                hiveFormattedDataSet.printSchema();
                hiveFormattedDataSet.createOrReplaceTempView("temp");
                //appending to table, if table not found create and append.
               // if (!Arrays.stream(spark.sqlContext().tableNames()).anyMatch(tableName -> tableName.equalsIgnoreCase(PropertiesUtils.getPropertyValue(HIVE_APP_TABLE)))) {
                    hiveFormattedDataSet.write().mode("append").format("hive").saveAsTable(PropertiesUtils.getPropertyValue(HIVE_APP_TABLE));
                //}
                // count of in hive table
                String countQuery="SELECT COUNT(*) FROM "+PropertiesUtils.getPropertyValue(HIVE_APP_DATABASE)+"."+PropertiesUtils.getPropertyValue(HIVE_APP_TABLE);
                spark.sql(countQuery).show();
                List<String> queries=Arrays.asList(PropertiesUtils.getPropertyValue(QUERIES).split("\\^"));
                for (String query:queries) {
                    LOGGER.debug("Running :: {} "+query);
                    Dataset result=spark.sql(query);
                    result.show();
                }
            } else {
                LOGGER.error("NO URL provided !, can't run batch!");
            }
            spark.stop();
        } catch (Exception e) {
            LOGGER.error("Error occurred while running job !!  errorMessage:: {}", e.getMessage(), e);
        }
    }
}
