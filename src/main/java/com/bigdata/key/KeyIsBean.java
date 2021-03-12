package com.bigdata.key;

import com.bigdata.bean.TestKeyBean;
import com.bigdata.serializer.TestKeyBeanSchema;
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
public class KeyIsBean {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        //分别读取kafka和文件中的数据
        Properties properties = new Properties();
        //这两个的properties是必须传递的
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "consumer01");
        FlinkKafkaConsumer010<TestKeyBean> consumer010 = new FlinkKafkaConsumer010<>("test",
                new TestKeyBeanSchema(),
                properties);
        //可以设置kafka读取数据的位置，比如设置读取最新的
        consumer010.setStartFromLatest();

        DataStreamSource<TestKeyBean> kafkaSource = executionEnvironment.addSource(consumer010);
        //从hdfs文件中读取
        DataStreamSource<String> stringDataStreamSource = executionEnvironment.addSource(new FileCountryDictSourceFunction());

        //对kafka中读取的值进行keyby
        KeyedStream<TestKeyBean, TestKeyBean> kafkaKeyedStream = kafkaSource.keyBy(new KeySelector<TestKeyBean, TestKeyBean>() {
            @Override
            public TestKeyBean getKey(TestKeyBean s) throws Exception {
                return s;
            }
        });

        //先将传入的数据转化成一个元组,key为TestKeyBean
        KeyedStream<Tuple2<TestKeyBean, String>, TestKeyBean> fileKeyedStream = stringDataStreamSource.map(new MapFunction<String, Tuple2<TestKeyBean, String>>() {
            @Override
            public Tuple2<TestKeyBean, String> map(String s) throws Exception {
                String[] s1 = s.split(" ");
                return Tuple2.of(new TestKeyBean(s1[0]), s1[1]);
            }
        }).keyBy(new KeySelector<Tuple2<TestKeyBean, String>, TestKeyBean>() {
            @Override
            public TestKeyBean getKey(Tuple2<TestKeyBean, String> stringStringTuple2) throws Exception {
                return stringStringTuple2.f0;
            }
        });

        //将两个key的stream进行合并
        ConnectedStreams<Tuple2<TestKeyBean, String>, TestKeyBean> connect = fileKeyedStream.connect(kafkaKeyedStream);

        //处理两个key，
        SingleOutputStreamOperator<String> process = connect.process(new KeyedCoProcessFunction<TestKeyBean, Tuple2<TestKeyBean, String>, TestKeyBean, String>() {
            private Map<String, String> map = new HashMap<>();

            @Override
            public void processElement1(Tuple2<TestKeyBean, String> value, Context ctx, Collector<String> out) throws Exception {
                map.put(value.f0.getRecord(), value.f1);
                out.collect(value.toString());
            }

            /**
             * 当前流的key是 TestKeyBean对象
             * 如何判断是同一个key
             * @param value
             * @param ctx
             * @param out
             * @throws Exception
             */
            @Override
            public void processElement2(TestKeyBean value, Context ctx, Collector<String> out) throws Exception {
                String countryName = map.get(ctx.getCurrentKey().getRecord());
                String outStr = countryName == null ? "no math" : countryName;
                out.collect(outStr);
            }
        });

        process.print();
        executionEnvironment.execute("");
    }
}
