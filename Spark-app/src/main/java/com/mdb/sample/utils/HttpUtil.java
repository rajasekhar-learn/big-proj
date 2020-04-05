package com.mdb.sample.utils;

import lombok.experimental.UtilityClass;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import static com.mdb.sample.constants.ModuleConstants.FILE_DOWNLOAD_TIMEOUT;

/**
 * HttpUtil provides API to handle any http related functionality like download file etc needed.
 */
@UtilityClass
public class HttpUtil {

    public static void downloadFile(String fileName, String url) throws IOException {
        FileUtils.copyURLToFile(
                new URL(url),
                new File(fileName),
                Integer.parseInt(PropertiesUtils.getPropertyValue(FILE_DOWNLOAD_TIMEOUT)),
                Integer.parseInt(PropertiesUtils.getPropertyValue(FILE_DOWNLOAD_TIMEOUT)));
    }

}
