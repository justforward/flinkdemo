package com.bigdata.source.input;

import com.bigdata.operator.speech.MessageTans;
import lombok.val;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.Properties;

/**
 * @author wangtan
 * @Date 2021/2/2
 */
public class KafkaInputStringToBean {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        val properties=new Properties();
        //这两个的properties是必须传递的
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "consumer01");
        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer010<>("test",
                        new SimpleStringSchema(),
                        properties));
        stream.map(new MessageTans());

        stream.print();
        env.execute("mm");
    }
}
