package com.mdb.sample.utils;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
@UtilityClass
public class QueryUtils {

    public static void executeQueriesAndSaveResultsToTables(List<String> queries, List<String> tables){
        for (int index=0;index<queries.size();index++) {
            String query=queries.get(index);
            String results_table=tables.get(index);
            log.debug("Running query :: {}, results table :: {}  ",query,results_table);
            SparkUtils.executeSparkQueryAndStoreToTable(query,results_table);
        }
    }
}
