package com.bigdata;

import com.bigdata.kafka.KafkaConfig;
import lombok.val;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @author wangtan
 * @Date 2021/3/10
 * 第一个demo 统计 某个单词出现的次数
 */
public class FirstDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度，就出现几个在跑
        env.setParallelism(8);
        //从kafka中读取数据源
        // DataStreamSource<String> dataStream = env.addSource(new FlinkKafkaConsumer010<String>("test", new SimpleStringSchema(), KafkaConfig.sourceConfig()));

        //从本地读取
        //终端输入：nc -lk 6666
        DataStreamSource<String> localhost = env.socketTextStream("localhost", 6666);

        //1、 lambda表达式的形式
        /*
        //flatMap 数据切分操作
        //List("a b", "c d").flatMap(line ⇒ line.split(" "))结果是 List(a, b, c, d)。
        SingleOutputStreamOperator<String> stringSingleOutputStreamOperator = localhost.flatMap((String value, Collector<String> out) -> {
            Arrays.stream(value.split(" ")).forEach(word -> {
                out.collect(word);
            });
        }).returns(Types.STRING);
        SingleOutputStreamOperator<Tuple2<String, Integer>> returns = stringSingleOutputStreamOperator
                .map(f ->
                        //将切换的数据转成元组，然后分组
                        //元组的概念 ：在数据库中一行数据代表一个元组
                        Tuple2.of(f, 1)

                ).returns(Types.TUPLE(Types.STRING, Types.INT));

        //按照第一位keyby 然后对第二位求和
        returns.keyBy(0).sum(1).print();*/

        //2、Function的用法
        SingleOutputStreamOperator<String> stringSingleOutputStreamOperator = localhost.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> out) throws Exception {
                String[] s1 = s.split(" ");
                for (String value : s1) {
                    out.collect(value);
                }
            }
        });

        //对从数据源中读取的单词进行统计
        SingleOutputStreamOperator<Tuple2<String, Integer>> map = stringSingleOutputStreamOperator.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value, 1);
            }
        });

        //根据tuple存储的单词进行分组，然后统计
        SingleOutputStreamOperator<Tuple2<String, Integer>> f0 = map.keyBy(0).sum(1);
        f0.print();

        //richFunction的使用
        SingleOutputStreamOperator<Tuple2<String, Integer>> tuple2SingleOutputStreamOperator = localhost.flatMap(new RichFlatMapFunction<String, Tuple2<String, Integer>>() {

            private String name = null;

            //初始化（初始参数）的时候执行一次
            @Override
            public void open(Configuration parameters) throws Exception {
                //添加前缀
                name = "xx_";
            }

            //在所有的任务都执行完后执行的方法
            @Override
            public void close() throws Exception {
                //执行完所有的任务之后置为null
                name = null;
            }

            //根据并行度（比如该程序中并行度为8），有8个在并行执行该方法
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> out) throws Exception {
                //得到当前task（并行度是8，8个task在执行）的标识
                int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
                String[] s1 = s.split(" ");
                for (String value : s1) {
                    out.collect(Tuple2.of(name + value, 1));
                }
            }
        });

        KeyedStream<Tuple2<String, Integer>, String> tuple2StringKeyedStream = tuple2SingleOutputStreamOperator.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                //根据第一个元素进行分组
                return stringIntegerTuple2.f0;
            }
        });


        env.execute("flatMap");

        System.out.println("demo 启动！");

    }


}
