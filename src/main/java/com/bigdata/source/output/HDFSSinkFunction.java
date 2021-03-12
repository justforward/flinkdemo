package com.bigdata.source.output;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * @author wangtan
 * @Date 2021/3/12
 */
public class HDFSSinkFunction extends RichSinkFunction<String> {
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }
}
