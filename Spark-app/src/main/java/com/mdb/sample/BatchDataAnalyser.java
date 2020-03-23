package com.mdb.sample;


import com.mdb.sample.utils.HDFSUtil;
import com.mdb.sample.utils.HttpUtil;
import com.mdb.sample.utils.PropertiesUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.mdb.sample.constants.ModuleConstants.*;

/**
 * This class is driver . which creates RDD and generate logical and physical planes and collect results from executors.
 * this driver is designed to load data from URL value and store in to hive , run the queries configured from properties file.
 * and store results in hive and mysql tables.
 *
 * for each run driver program append the data to existing hive table to run cumulative analysis.(if db and tale is same as per configuration of run)
 * due to this we can analyse hole data loaded till that point to have accurate results.
 *
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
                    .appName("Spark app")
                    .config("spark.sql.warehouse.dir", warehouseLocation)
                    .enableHiveSupport()
                    .getOrCreate();

            String url = args.length > 0 ? args[0] : "";
            if (StringUtils.isNotEmpty(url)) {
                //load csv from URL to HDFS.
                String fileLocation=PropertiesUtils.getPropertyValue(FILE_DOWNLOAD_LOCATION)+System.currentTimeMillis()+"-data.csv";
                HttpUtil.downloadFile(fileLocation,url);
                if(FileUtils.sizeOf(new File(fileLocation))==0){
                    LOGGER.error("No content, Empty file received !!");
                    throw new IllegalStateException("No content, Empty file received !!");
                }
                HDFSUtil.createHdfsDir(PropertiesUtils.getPropertyValue(HADOOP_FILE_COPY_PATH));
                HDFSUtil.copyFileToHDFS(fileLocation,PropertiesUtils.getPropertyValue(HADOOP_FILE_COPY_PATH));
                String hdfsFilePath=PropertiesUtils.getPropertyValue(HADOOP_FILE_COPY_PATH)+fileLocation.substring(fileLocation.lastIndexOf(File.separator)+1);
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
                Map<String,String> queeyAndTablesMap=PropertiesUtils.getPropertyKeyAndValuesByPrefix(QUERIES,true);
                List<String> queries=Arrays.asList(queeyAndTablesMap.get("statements").split("\\^"));
                List<String> tables=Arrays.asList(queeyAndTablesMap.get("tables").split(","));
                if(queries.size()!=tables.size()){
                    LOGGER.error("Number of queries and result tables count mismatch!!");
                    throw new IllegalArgumentException("queries and tables values mismatch!!");
                }
                for (int i=0;i<queries.size();i++) {
                    LOGGER.debug("Running :: {} "+queries.get(i));
                    Dataset result=spark.sql(queries.get(i));
                    result.show();
                    result.write().mode(SaveMode.Overwrite).format("hive").saveAsTable(tables.get(i));
                    //storing results to mysql as well , to make data available in case of hive issues and to test JDBC storage format.
                    result.write().format("jdbc").mode(SaveMode.Overwrite)
                            .options(PropertiesUtils.getPropertyKeyAndValuesByPrefix(APP_RESULTS_DB_PREFIX,true))
                            .option("dbtable",tables.get(i))//to support different query result tables
                            .save();
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
