package com.bigdata.utils;

import lombok.SneakyThrows;

import java.io.InputStream;
import java.util.Properties;

/**
 * @program: nsky-flink
 * @description:
 * @author: liuyu
 * @create: 2020-08-30 23:15
 **/
public class PropertiesUtils {

    @SneakyThrows
    public static Properties load(String location) {
        Properties properties = new Properties();
        InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(location);
        properties.load(inputStream);
        return properties;
    }
}
