package com.bigdata.timestamp;

import lombok.val;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author wangtan
 * @Date 2021/2/5
 */
public class TimeStampLaterData6 {
    private static final OutputTag<String> late = new OutputTag<String>("late", BasicTypeInfo.STRING_TYPE_INFO);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
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

        val process = stringDataStreamSource.keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String value) throws Exception {
                String[] split = value.split("\t");
                return split[1];
            }
        }).timeWindow(Time.seconds(2))
                .allowedLateness(Time.seconds(2))//设置等待晚两秒的数据
                .sideOutputLateData(late)//超时的数据将会打上分流器标签
                .process(new ProcessWindowFunction<String, String, String, TimeWindow>() {

                    /**
                     * @param s
                     * @param context 输出关于窗口的信息
                     * @param elements 目前进入窗口的数据
                     * @param out
                     * @throws Exception
                     */
                    @Override
                    public void process(String s, Context context, Iterable<String> elements, Collector<String> out) throws Exception {
                        System.out.println("subtask:" + getRuntimeContext().getIndexOfThisSubtask() +
                                ",start:" + context.window().getStart() +
                                ",end" + context.window().getEnd() +
                                ",watermarks:" + context.currentWatermark() +
                                ".currentTime" + System.currentTimeMillis());
                        elements.forEach(v -> {
                            System.out.println("windows-->" + v);
                            //正常的数据，发送出去
                            out.collect("on time:"+v);
                        });

                    }
                });
        //处理准时的数据
        process.print();
        DataStream<String> lateOutPut = process.getSideOutput(late);
        lateOutPut.map(new MapFunction<String, String>() {

            @Override
            public String map(String s) throws Exception {
                return "late:" + s;
            }
        }).print();

        env.execute();
    }
}
