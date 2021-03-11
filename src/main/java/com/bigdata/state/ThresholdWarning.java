package com.bigdata.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * @Auther wangtan
 * @Date 2020/11/6
 */
public class ThresholdWarning extends RichFlatMapFunction<Tuple2<String, Long>, Tuple2<String, List<Long>>> {
    //通过ListState来存储非正常数据的状态
    /**
     * ListState：存储列表类型的状态。
     * 可以使用 add(T) 或 addAll(List) 添加元素；并通过 get() 获得整个列表。
     */
    private transient ListState<Long> abnormalData;
    //需要监控的阈值
    private Long threshold;
    //触发报警的次数
    private Long numberOfTimes;

    ThresholdWarning(Long threshold,Long numberOfTimes){
        this.threshold=threshold;
        this.numberOfTimes=numberOfTimes;
    }
    @Override
    public void open(Configuration parameters) throws Exception {
        //通过状态名称（句柄）获取状态实例，如果不存在则会自动创建
        abnormalData=getRuntimeContext().getListState(new ListStateDescriptor<>("abnormalData",Long.class));
    }

    //处理输入数据的逻辑
    @Override
    public void flatMap(Tuple2<String, Long> value, Collector<Tuple2<String, List<Long>>> out) throws Exception {
        Long inputValue=value.f1;
        //如果输入的数据超出设置的阈值，更新状态信息
        if(inputValue>=threshold){
            abnormalData.add(inputValue);
        }
        ArrayList<Long> list= Lists.newArrayList(abnormalData.get().iterator());
        //如果不正常的数据到达一定的次数，则会输出报警信息
        if(list.size()>=numberOfTimes){
            out.collect(Tuple2.of(value.f0+" 超出指定阈值",list));
            //报警信息输出后，清空状态
            abnormalData.clear();
        }

    }
}
