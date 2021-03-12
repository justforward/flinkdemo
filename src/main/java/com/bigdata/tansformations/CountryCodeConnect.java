package com.bigdata.tansformations;

import com.bigdata.source.input.FileCountryDictSourceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author wangtan
 * @Date 2021/3/11
 * 利用connect 解决了 耦合问题
 * 但是没有解决并发度的问题
 */
public class CountryCodeConnect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        //分别读取kafka和文件中的数据
        Properties properties = new Properties();
        //这两个的properties是必须传递的
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "consumer01");
        FlinkKafkaConsumer010<String> consumer010 = new FlinkKafkaConsumer010<>("test",
                new SimpleStringSchema(),
                properties);
        //可以设置kafka读取数据的位置，比如设置读取最新的
        consumer010.setStartFromLatest();

        DataStreamSource<String> kafkaSource = executionEnvironment.addSource(consumer010);
        //从hdfs文件中读取
        DataStreamSource<String> stringDataStreamSource = executionEnvironment.addSource(new FileCountryDictSourceFunction());

        ConnectedStreams<String, String> connect = stringDataStreamSource.connect(kafkaSource);
        SingleOutputStreamOperator<String> process = connect.process(new CoProcessFunction<String, String, String>() {
            private Map<String, String> map = new HashMap<String, String>();

            /**
             * stringDataStreamSource的数据，文件来的数据存放到map里面
             * @param value
             * @param ctx
             * @param out
             * @throws Exception
             */
            @Override
            public void processElement1(String value, Context ctx, Collector<String> out) throws Exception {
                String[] s = value.split(" ");
                //从文件中得到的数据,存储到map中
                map.put(s[0], s[1]);
                //往下传递
                out.collect(value);
            }

            /**
             * kafkaSource的数据，从map中读取数据
             * @param value
             * @param ctx
             * @param out
             * @throws Exception
             */
            @Override
            public void processElement2(String value, Context ctx, Collector<String> out) throws Exception {
                //从kafka中读取的数据
                String countryName = map.get(value);
                String s1 = countryName == null ? "no match" : countryName;
                out.collect(s1);
            }
        });

        process.print();
        executionEnvironment.execute("connect");

    }
}
