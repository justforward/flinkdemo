package com.bigdata.timestamp;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author wangtan
 * @Date 2021/2/4
 */
public class TimeStampBoundedOutOfOrderness5 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env=StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> stringDataStreamSource = env.addSource(new SourceFunction<String>() {
            private Boolean isCancel = true;
            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                while (isCancel) {
                    //返回的是，毫秒的时间戳！
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
        //根据传入的参数Time.seconds(10)，判断Watermark延迟的时间，该例子中延迟10s
        stringDataStreamSource.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(10)) {
            @Override
            public long extractTimestamp(String element) {
                String[] split = element.split("\t");
                long timestamp =Long.valueOf(split[0]);
                return timestamp;
            }
        });
        env.execute("use Api to get watermark and eventTime by bounded out of orderness");

    }

}
