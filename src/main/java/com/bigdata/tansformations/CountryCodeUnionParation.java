package com.bigdata.tansformations;

import com.bigdata.bean.TestKeyBean;
import com.bigdata.partion.MyPartitoner;
import com.bigdata.serializer.TestKeyBeanSchema;
import com.bigdata.source.input.FileCountryDictSourceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
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
public class CountryCodeUnionParation {
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

        //对kafka中读取的值进行先进行分区，然后keyby-指定key是什么
        //返回的结果并非keyby的值，而是根据分区，CN的在一个分区，其他的在另一个分区中。
        //但是在这种情况下用了两个分区
        DataStream<TestKeyBean> kafkaKeyedStream = kafkaSource.partitionCustom(new MyPartitoner(), new KeySelector<TestKeyBean, TestKeyBean>() {
            @Override
            public TestKeyBean getKey(TestKeyBean value) throws Exception {
                return value;
            }
        });

        //返回的值，不是keyby的值
        DataStream<Tuple2<TestKeyBean, String>> countryDict = stringDataStreamSource.map(new MapFunction<String, Tuple2<TestKeyBean, String>>() {
            @Override
            public Tuple2<TestKeyBean, String> map(String s) throws Exception {
                String[] s1 = s.split(" ");
                return Tuple2.of(new TestKeyBean(s1[0]), s1[1]);
            }
        }).partitionCustom(new MyPartitoner(), new KeySelector<Tuple2<TestKeyBean, String>, TestKeyBean>() {
            @Override
            public TestKeyBean getKey(Tuple2<TestKeyBean, String> value) throws Exception {
                return value.f0;
            }
        });

        //将两个key的stream进行合并，并非keyby之后的值，
        ConnectedStreams<Tuple2<TestKeyBean, String>, TestKeyBean> connect = countryDict.connect(kafkaKeyedStream);

        SingleOutputStreamOperator<String> process = connect.process(new CoProcessFunction<Tuple2<TestKeyBean, String>, TestKeyBean, String>() {
            private Map<String, String> map = new HashMap<>();

            @Override
            public void processElement1(Tuple2<TestKeyBean, String> value, Context ctx, Collector<String> out) throws Exception {
                map.put(value.f0.getRecord(), value.f1);
                out.collect(value.toString());
            }

            @Override
            public void processElement2(TestKeyBean value, Context ctx, Collector<String> out) throws Exception {
                String countryName = map.get(value.getRecord());
                String s = countryName == null ? "no match" : countryName;
                out.collect(s);
            }
        });
        process.print();
        executionEnvironment.execute("");
    }
}
