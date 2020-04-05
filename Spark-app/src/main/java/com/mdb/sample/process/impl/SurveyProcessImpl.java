package com.mdb.sample.process.impl;

import com.mdb.sample.process.BatchProcess;
import com.mdb.sample.utils.PropertiesUtils;
import com.mdb.sample.utils.QueryUtils;
import com.mdb.sample.utils.SparkUtils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.mdb.sample.constants.ModuleConstants.*;
import static com.mdb.sample.utils.FileHandler.downloadAndPushFileToHDFS;

@Slf4j
public class SurveyProcessImpl implements BatchProcess, Serializable {


    @SneakyThrows
    @Override
    public void executeBatchProcess(Map<String, Object> params) {
        String url = String.valueOf(params.get(URL));
        if (StringUtils.isNotEmpty(url)) {
            String hdfsFilePath = downloadAndPushFileToHDFS(url);
            log.debug("File location in HDFS :: {} ", hdfsFilePath);
            //dataset creation from HDFS path
            Dataset<Row> data = SparkUtils.createDataSetFromSource(hdfsFilePath,"csv");
            //given data set column name has issues(like space added to High_Confidence_Limit), due to this only initial load works,next appends fail. to support appending
            //data to hive table in next runs, i am converting raw inferred dataset to hive supported one, by trimming and converting columns to lower case.
            Dataset<Row> hiveFormattedDataSet = data.toDF(Arrays.asList(data.columns()).stream().map(x -> x.toLowerCase().trim()).toArray(size -> new String[size]));
            hiveFormattedDataSet.printSchema();

            hiveFormattedDataSet.createOrReplaceTempView("temp");
            hiveFormattedDataSet.write().mode("append").format("hive").saveAsTable(PropertiesUtils.getPropertyValue(HIVE_APP_TABLE));

            // count of in hive table
            StringBuilder countQuery=new StringBuilder();
            countQuery.append("SELECT COUNT(*) FROM ").append(PropertiesUtils.getPropertyValue(HIVE_APP_DATABASE))
                    .append(".").append(PropertiesUtils.getPropertyValue(HIVE_APP_TABLE));
            SparkUtils.ranShowQuery(countQuery.toString());

            Map<String, String> queryAndTablesMap = PropertiesUtils.getPropertyKeyAndValuesByPrefix(QUERIES, true);
            List<String> queries = Arrays.asList(queryAndTablesMap.get("statements").split("\\^"));
            List<String> tables = Arrays.asList(queryAndTablesMap.get("tables").split(","));
            if (queries.size() != tables.size()) {
                log.error("Number of queries and result tables count mismatch!!");
                throw new IllegalArgumentException("queries and tables values mismatch!!");
            }
            QueryUtils.executeQueriesAndSaveResultsToTables(queries, tables);
        } else {
            log.error("NO URL provided !, can't run batch!");
        }
    }


}
