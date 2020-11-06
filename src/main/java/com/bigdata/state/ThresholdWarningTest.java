package com.bigdata.state;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Auther wangtan
 * @Date 2020/11/6
 */
public class ThresholdWarningTest {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env= StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<String, Long>> tuple2DataStreamSource = env.fromElements(
                Tuple2.of("a", 50L), Tuple2.of("a", 80L), Tuple2.of("a", 400L),
                Tuple2.of("a", 100L), Tuple2.of("a", 200L), Tuple2.of("a", 200L),
                Tuple2.of("b", 100L), Tuple2.of("b", 200L), Tuple2.of("b", 200L),
                Tuple2.of("b", 500L), Tuple2.of("b", 600L), Tuple2.of("b", 700L));
        tuple2DataStreamSource
                .keyBy(0)
                .flatMap(new ThresholdWarning(100L, 3L))  // 超过100的阈值3次后就进行报警
                .printToErr();
        env.execute("Managed Keyed State");


    }
}
