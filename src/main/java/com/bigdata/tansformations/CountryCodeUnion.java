package com.bigdata.tansformations;

import com.bigdata.source.input.FileCountryDictSourceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author wangtan
 * @Date 2021/3/11
 * 该代码存在两个问题的：
 *  1)不能并行：如果并行度大于kafka的partition的时候，这意味着有的槽无法读到kafka数据
 *      那么如果由这个槽执行任务，会导致任务失败。应该命中的，但是结果没有命中成功。
 *  2）耦合问题：不方便区分数据流从哪来。
 *  source读取的一条数据信息格式如下：
 *  1）从文件流中读取的一条数据形式为：AR 阿根廷 Argentina
 *  2）从kafka中读取的一条数据形式为：AR
 *
 */
public class CountryCodeUnion {
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



        DataStream<String> union = stringDataStreamSource.union(kafkaSource);

        //将合并的数据流进行处理，每个并行（每个槽）都会执行
        SingleOutputStreamOperator<String> process = union.process(new ProcessFunction<String, String>() {
            //该map是存在于某个槽上的，对于其他的槽是不共享的
            private Map<String, String> map = new HashMap<String, String>();

            @Override
            public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                //两个流的合并之后，只有一个in参数。需要区分数据来自于哪个流
                //方法一、根据两个流中不同的数据格式来区分！
                //比如从文件流中读取的一条数据形式为：AR 阿根廷 Argentina
                //从kafka中读取的一条数据形式为：a
                String[] s = value.split(" ");
                if (s.length > 1) {
                    //从文件中得到的数据,存储到map中
                    map.put(s[0], s[1]);
                    //往下传递
                    out.collect(value);
                } else {
                    //从kafka中读取的数据
                    String countryName = map.get(value);
                    String s1 = countryName == null ? "no match" : countryName;
                    out.collect(s1);
                }
            }
        });
        process.print();


        executionEnvironment.execute("");
    }
}
