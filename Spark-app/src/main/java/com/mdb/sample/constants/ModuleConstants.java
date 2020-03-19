package com.mdb.sample.constants;

public final class ModuleConstants {
    /**
     * private constructor.
     */
    private ModuleConstants(){
        throw new IllegalStateException("constants class");
    }

    public static final String HIVE_WAREHOUSE_LOCATION="app.env.hive.warehouselocation";
    public static final String HIVE_APP_DATABASE="app.hive.database";
    public static final String HIVE_APP_TABLE="app.hive.table";
    public static final String FILE_DOWNLOAD_TIMEOUT="app.file.download.timeoutInMS";
    public static final String FILE_DOWNLOAD_LOCATION="app.file.download.location";
    public static final String HADOOP_CORE_CONFIG_PATH="app.core.config.path";
    public static final String HADOOP_FILE_COPY_PATH="app.hdfs.copy.path";
    public static final String QUERIES="app.queries";
}
