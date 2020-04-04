package com.mdb.sample.utils;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;

import java.io.File;

import static com.mdb.sample.constants.ModuleConstants.FILE_DOWNLOAD_LOCATION;
import static com.mdb.sample.constants.ModuleConstants.HADOOP_FILE_COPY_PATH;

@Slf4j
public class FileHandler {

    /**
     * downloadAndPushFileToHDFS method will download file from url, and push that file to predefined hdfs
     *  location.
     * @param url file url
     * @return hdfs file location
     */
    @SneakyThrows
    public static String downloadAndPushFileToHDFS(String url) {
        //load csv from URL to HDFS.
        StringBuilder fileLocation=new StringBuilder();
        fileLocation.append(PropertiesUtils.getPropertyValue(FILE_DOWNLOAD_LOCATION))
                .append(System.currentTimeMillis()).append("-data.csv");
        HttpUtil.downloadFile(fileLocation.toString(), url);
        if (FileUtils.sizeOf(new File(fileLocation.toString())) == 0) {
            log.error("No content, Empty file received !!");
            throw new IllegalStateException("No content, Empty file received !!");
        }
        HDFSUtil.createHdfsDir(PropertiesUtils.getPropertyValue(HADOOP_FILE_COPY_PATH));
        HDFSUtil.copyFileToHDFS(fileLocation.toString(), PropertiesUtils.getPropertyValue(HADOOP_FILE_COPY_PATH));
        return new StringBuilder().append(PropertiesUtils.getPropertyValue(HADOOP_FILE_COPY_PATH))
                .append(fileLocation.substring(fileLocation.lastIndexOf(File.separator) + 1)).toString();
    }
}
