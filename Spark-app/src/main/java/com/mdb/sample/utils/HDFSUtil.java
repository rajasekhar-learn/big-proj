package com.mdb.sample.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

import static com.mdb.sample.constants.ModuleConstants.HADOOP_CORE_CONFIG_PATH;

/**
 * HDFS utility methods.
 */
public class HDFSUtil {

    private static final Configuration conf = new Configuration();
    static {
        conf.addResource(new Path(PropertiesUtils.getPropertyValue(HADOOP_CORE_CONFIG_PATH)+"core-site.xml"));
        conf.addResource(new Path(PropertiesUtils.getPropertyValue(HADOOP_CORE_CONFIG_PATH)+"hdfs-site.xml"));
        conf.addResource(new Path(PropertiesUtils.getPropertyValue(HADOOP_CORE_CONFIG_PATH)+"hive-site.xml"));
        conf.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl",org.apache.hadoop.fs.LocalFileSystem.class.getName());
    }

    /**
     * local file copy to HDFS.
     * @param localPath local file system file path.
     * @param hdfsPath hdfs file system path
     * @throws IOException
     */
    public static void copyFileToHDFS(String localPath,String hdfsPath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        fs.copyFromLocalFile(new Path(localPath),new Path(hdfsPath));
    }

    /**
     * creates hdfs location.
     * @param hdfsDir location to be created.
     * @throws IOException
     */
    public static void createHdfsDir(String hdfsDir)throws IOException{
        FileSystem dfs = FileSystem.get(conf);
        Path destinationDir = new Path(hdfsDir);
        if(!dfs.exists(destinationDir)) {
            dfs.mkdirs(destinationDir);
        }
    }


}
