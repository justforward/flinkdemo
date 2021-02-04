package com.bigdata.timestamp;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * @author wangtan
 * @Date 2021/2/4
 */
public class TimeStampWatermarkByTimeMethod2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env=StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> stringDataStreamSource = env.addSource(new SourceFunction<String>() {
            private Boolean isCancel = true;
            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                while (isCancel) {
                    long currentTime = System.currentTimeMillis();
                    //模拟数据源 ，数据源为string类型
                    String s = currentTime + "\thetu\t" + (currentTime - 1000);
                    //source直接发送数据
                    ctx.collect(s);
                    Thread.sleep(1000);
                }
            }
            @Override
            public void cancel() {
                isCancel = false;
            }
        });
        stringDataStreamSource.print();
        stringDataStreamSource.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<String>() {
            private Long watermarkTime;

            /**
             * 设置Watermark
             * @return Watermark
             */
            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(watermarkTime);
            }
            /**
             * 从给定的数据中抽取eventTime返回
             * @param element
             * @param previousElementTimestamp
             * @return eventTime
             */
            //设置eventTime
            @Override
            public long extractTimestamp(String element, long previousElementTimestamp) {
                String[] split=element.split("\t");
                Long eventTime=Long.valueOf(split[0]);
                //watermark 比eventTime晚1s
                watermarkTime=eventTime-1000;
                return eventTime;
            }
        });
        env.execute();

    }
}
