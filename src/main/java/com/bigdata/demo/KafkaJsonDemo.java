package com.bigdata.demo;

import com.bigdata.bean.CycleTagBean;
import com.bigdata.kafka.KafkaConfig;
import com.bigdata.serializer.HmCycleTagSchema;
import com.bigdata.serializer.HmCycleTagSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;

import java.util.Properties;

/**
 * @Auther wangtan
 * @Date 2020/10/16
 */
public class KafkaJsonDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //1。 输入数据
        Properties properties = KafkaConfig.sourceConfig();
        //参数：1）topic
        //      2）序列化的类
        //     3）配置kafka的信息
        DataStream<CycleTagBean> stream = env
                .addSource(new FlinkKafkaConsumer010<>("test",
                        new HmCycleTagSchema(),
                        properties));
        stream.print();
        Properties out=new Properties();
        out.setProperty("bootstrap.servers", "localhost:9092");
        //2。输出数据到一个topic里面
        //参数：1）topic 2）指定序列化的输出 3）指定kafka的配置信息 必须包含kafka的borken信息
        FlinkKafkaProducer010 hmOrderWithCycle = new FlinkKafkaProducer010("consumerTest",
                new HmCycleTagSink(), properties);
        stream.addSink(hmOrderWithCycle);
        env.execute("kafka Json input");
    }
}
