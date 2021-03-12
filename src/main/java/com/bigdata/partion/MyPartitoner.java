package com.bigdata.partion;

import com.bigdata.bean.TestKeyBean;
import org.apache.flink.api.common.functions.Partitioner;

/**
 * @author wangtan
 * @Date 2021/3/12
 */
public class MyPartitoner implements Partitioner<TestKeyBean> {

    /**
     * @param key key
     * @param numPartitions 分区所在的num
     * @return
     */
    @Override
    public int partition(TestKeyBean key, int numPartitions) {
        if (key.getRecord().contains("CN")){
            return 0;
        }else{
            return 1;
        }
    }
}
