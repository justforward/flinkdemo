package com.bigdata.tansformations;

import com.bigdata.source.input.FileCountryDictSourceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author wangtan
 * @Date 2021/3/12
 */
public class CountryCodeUnionKeyByAndConnect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment=StreamExecutionEnvironment.getExecutionEnvironment();
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

        //对kafka中读取的值进行keyby
        KeyedStream<String, String> kafkaKeyedStream = kafkaSource.keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String s) throws Exception {
                return s;
            }
        });

        //先将传入的数据转化成一个元组
        KeyedStream<Tuple2<String, String>, String> fileKeyedStream = stringDataStreamSource.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String s) throws Exception {
                String[] s1 = s.split(" ");
                return Tuple2.of(s1[0], s1[1]);
            }
        }).keyBy(new KeySelector<Tuple2<String, String>, String>() {
            @Override
            public String getKey(Tuple2<String, String> stringStringTuple2) throws Exception {
                return stringStringTuple2.f0;
            }
        });

        //将两个key的stream进行合并
        ConnectedStreams<Tuple2<String, String>, String> connect = fileKeyedStream.connect(kafkaKeyedStream);

        //处理两个key
        SingleOutputStreamOperator<String> process = connect.process(new KeyedCoProcessFunction<String, Tuple2<String, String>, String, String>() {
            private Map<String, String> map = new HashMap<>();

            @Override
            public void processElement1(Tuple2<String, String> value, Context ctx, Collector<String> out) throws Exception {
                map.put(value.f0, value.f1);
                out.collect(value.toString());
            }

            @Override
            public void processElement2(String value, Context ctx, Collector<String> out) throws Exception {
                //从kafka中读取的数据
                String countryName = map.get(value);
                String s1 = countryName == null ? "no match" : countryName;
                out.collect(s1);
            }
        });

        process.print();
        executionEnvironment.execute("");
    }
}
