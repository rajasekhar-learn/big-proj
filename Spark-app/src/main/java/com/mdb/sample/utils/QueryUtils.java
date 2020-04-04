package com.mdb.sample.utils;

import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public class QueryUtils {

    public static void executeQueriesAndSaveResultsToTables(List<String> queries, List<String> tables){
        for (int index=0;index<queries.size();index++) {
            log.debug("Running :: {} ",queries.get(index));
            System.out.println("Running query :: {} "+queries.get(index));
            SparkUtils.executeSparkQueryAndStoreToTable(queries.get(index),tables.get(index));
        }
    }
}
