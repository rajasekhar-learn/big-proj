package com.mdb.sample;

import com.mdb.sample.utils.ProcessUtil;
import com.mdb.sample.utils.SparkUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

import static com.mdb.sample.constants.ModuleConstants.URL;

/**
 * This class is driver . which creates RDD and generate logical and physical planes and collect results from executors.
 * this driver is designed to load data from URL value and store in to hive , run the queries configured from properties file.
 * and store results in hive and mysql tables.
 * <p>
 * for each run driver program append the data to existing hive table to run cumulative analysis.(if db and tale is same as per configuration of run)
 * due to this we can analyse hole data loaded till that point to have accurate results.
 */
@Slf4j
public class Analyser {
    /**
     * Boot strapping point. expects list of argument, that program can expect to run.
     *
     * @param args expects 2 parameters 1st parameter processId second URL argument(arg[1]), from where data can be fetched.
     */
    public static void main(String[] args) {
        //eagerly creating spark session
        SparkSession spark = SparkUtils.getOrCreateSparkSession();
        //determine the process type. i am checking from static param from args.
        // this can be dynamic dynamic parameter based on use case.
        String processId = args.length > 1 ? args[0] : "";
        String url = args.length > 1 ? args[1] : "";
        Map<String, Object> params = new HashMap<>();
        params.put(URL, url);
        try {
            if(processId.matches("\\d+")) {
                ProcessUtil.invokeProcess(Integer.valueOf(processId), params);
            }else {
                log.error("Process id is invalid!! , please pass correct process id.");
            }
        } catch (Exception e) {
            log.error("Error occurred while running job !!  errorMessage:: {}", e.getMessage(), e);
        } finally {
            //stop spark session
            spark.stop();
        }
    }

}
