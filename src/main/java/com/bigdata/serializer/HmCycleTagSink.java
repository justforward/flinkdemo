package com.bigdata.serializer;

import com.alibaba.fastjson.JSONObject;
import com.bigdata.bean.CycleTagBean;
import org.apache.flink.api.common.serialization.SerializationSchema;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * @Auther wangtan
 * @Date 2020/10/19
 * 序列化：将对象转字节流（序列化）
 *
 */
public class HmCycleTagSink implements SerializationSchema<CycleTagBean> {

    @Override
    public byte[] serialize(CycleTagBean cycleTagBean) {
       //1。指定输出的某个属性
       /* JSONObject json = new JSONObject();
        json.put("bizType", cycleTagBean.getBizType());
        return json.toJSONString().getBytes(UTF_8);*/

        //2。输出该对象
        return cycleTagBean.getContent().toJSONString().getBytes(UTF_8);
    }
}
