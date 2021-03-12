package com.bigdata.source;

import com.bigdata.source.input.FileCountryDictSourceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author wangtan
 * @Date 2021/3/11
 */
public class FileSourceInput {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment=StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stringDataStreamSource = executionEnvironment.addSource(new FileCountryDictSourceFunction());
        stringDataStreamSource.print();
        executionEnvironment.execute(" read file from hadoop");
    }
}
