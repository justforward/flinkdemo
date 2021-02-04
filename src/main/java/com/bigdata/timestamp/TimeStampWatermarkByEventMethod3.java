package com.bigdata.timestamp;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * @author wangtan
 * @Date 2021/2/4
 */
public class TimeStampWatermarkByEventMethod3 {
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
        stringDataStreamSource.assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<String>() {
            private long maxOutOfOrderness=1000;
            /**
             * @param lastElement 得到的字段  可以根据字段值判断抽取Watermark的时机
             * @param extractedTimestamp 抽取的eventTime(由下面的方法返回)
             * @return
             */
            @Nullable
            @Override
            public Watermark checkAndGetNextWatermark(String lastElement, long extractedTimestamp) {
                String[] split = lastElement.split("\t");
                String data=split[1];
                if(data.equals("hetu")){
                    //遇到某个值的时候，开始构建Watermark
                    return new Watermark(extractedTimestamp-maxOutOfOrderness);
                }else{
                    //否则没有Watermark的返回
                    return null;
                }
            }

            /**
             * 抽取eventTime
             * @param element
             * @param previousElementTimestamp
             * @return
             */
            @Override
            public long extractTimestamp(String element, long previousElementTimestamp) {
                String[] split = element.split("\t");
                Long timestamp =Long.valueOf(split[0]);
                return timestamp;
            }
        });
        env.execute();

    }
}
