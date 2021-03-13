package com.bigdata.source.output;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author wangtan
 * @Date 2021/3/12
 * 将source的数据写入到HDFS中
 */
public class HDFSFile {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream("localhost", 6666);
        source.addSink(new HDFSSinkFunction());
        env.execute();
    }

}
