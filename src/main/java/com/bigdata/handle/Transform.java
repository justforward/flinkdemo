package com.bigdata.handle;

import com.bigdata.bean.CycleTagBean;
import com.bigdata.serializer.HmCycleTagSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.Properties;

/**
 * @Auther wangtan
 * @Date 2020/10/19
 */
public class Transform {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        //1。 输入数据
        Properties properties=new Properties();
        //这两个的properties是必须传递的
        //以逗号分隔的 Kafka broker 即：消息中间件存在的某个服务器，可能为多个服务器
        properties.setProperty("bootstrap.servers", "localhost:9092");
        //消费组 ID(自定义名称，不与其他的消费组同名即可)
        properties.setProperty("group.id", "consumer01");
        //参数：1）topic
        //      2）序列化的类
        //     3）配置kafka的信息
        DataStream<CycleTagBean> stream = env
                .addSource(new FlinkKafkaConsumer010<>("test",
                        new HmCycleTagSchema(),
                        properties));
        stream.print();
        //Reduce: 基于ReduceFunction进行滚动聚合，并向下游算子输出每次滚动聚合后的结果。
        //注意: Reduce会输出每一次滚动聚合的结果。
        //注意窗口的统计，每个窗口出现一个数值

        //flink的TimeCharacteristic枚举定义了三类值，
        // 分别是ProcessingTime、IngestionTime、EventTime
        //ProcessingTime是以operator处理的时间为准，它使用的是机器的系统时间来作为data stream的时间；
        // IngestionTime是以数据进入flink streaming data flow的时间为准；
        // EventTime是以数据自带的时间戳字段为准，应用程序需要指定如何从record中抽取时间戳字段
        //指定为EventTime的source需要自己定义event time以及emit watermark，或者在source之外通过assignTimestampsAndWatermarks在程序手工指定
        stream.filter(x->x!=null)
                .keyBy(x->x.getVehicleId())
                .timeWindow(Time.seconds(5))
                .reduce((s1,s2)-> s1.getCityId()>s2.getCityId()?s1:s2)
                .print();
        env.execute();
    }
}
