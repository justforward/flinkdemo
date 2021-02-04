package com.bigdata.timestamp;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.List;

/**
 * @author wangtan
 * @Date 2021/2/3
 */
public class TimeStampWatermarkMethod1 {
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
                    //切割数据源
                    String[] split = s.split("\t");
                    //得到数据源的时间戳
                    Long timestamp = Long.valueOf(split[0]);
                    String data = split[1];
                    Long waterMarks = Long.valueOf(split[2]);
                    //这句是指定eventTime 是字符串的哪个字段
                    ctx.collectWithTimestamp(data, timestamp);
                    //watermark设置的值要比eventTime小？
                    //1)从数据中获取
                    ctx.emitWatermark(new Watermark(waterMarks));
                    //2)自定义一个ctx
                    //ctx.emitWatermark(new Watermark(currentTime-1000));
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {
                isCancel = false;
            }
        });
        stringDataStreamSource.print();
        env.execute();

    }
}
