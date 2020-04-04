package com.mdb.sample.utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import static com.mdb.sample.constants.ModuleConstants.APP_RESULTS_DB_PREFIX;
import static com.mdb.sample.constants.ModuleConstants.HIVE_WAREHOUSE_LOCATION;

public class SparkUtils {
    private static SparkSession sparkSession = null;
    //hive warehouse location
    private static String warehouseLocation = PropertiesUtils.getPropertyValue(HIVE_WAREHOUSE_LOCATION);

    /**
     * spark session which is mandatory for application.
     * @return SparkSession
     */
    public static synchronized SparkSession getOrCreateSparkSession() {
        if (sparkSession == null) {
            sparkSession = SparkSession
                    .builder()
                    .master("local[*]")
                    .appName("Spark app")
                    .config("spark.sql.warehouse.dir", warehouseLocation)
                    .enableHiveSupport()
                    .getOrCreate();
        }
        return sparkSession;
    }

    /**
     * executeSparkQueryAndStoreToTable method will help to execute query and store results into corresponding table.
     * @param query query to be ran
     * @param resultsTableName results table
     */
    public static void executeSparkQueryAndStoreToTable(String query, String resultsTableName){
        Dataset result=getOrCreateSparkSession().sql(query);
        result.show();
        result.write().mode(SaveMode.Overwrite).format("hive").saveAsTable(resultsTableName);
        //storing results to mysql as well , to make data available in case of hive issues and to test JDBC storage format.
        result.write().format("jdbc").mode(SaveMode.Overwrite)
                .options(PropertiesUtils.getPropertyKeyAndValuesByPrefix(APP_RESULTS_DB_PREFIX,true))
                .option("dbtable",resultsTableName)//to support different query result tables
                .save();
    }

    /**
     * createDataSetFromSource method provide dataset from given formatted data.
     * @param dataSource data source path from where data set has to be created.
     * @param format format of data like csv, json, parquet..etc
     * @return resulting dataset will be returned.
     */
    public static Dataset<Row> createDataSetFromSource(String dataSource,String format){
        return getOrCreateSparkSession().read().format(format).option("header", "true")
                .option("inferSchema", "true")
                .load(dataSource);
    }

    /**
     * run show query
     * @param query query to be viewed
     */
    public static void ranQuery(String query){
        // count of in hive table
        getOrCreateSparkSession().sql(query).show();
    }

}
