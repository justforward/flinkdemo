package com.bigdata.serializer;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bigdata.bean.TestKeyBean;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/**
 * @author wangtan
 * @Date 2021/3/12
 */
public class TestKeyBeanSchema implements DeserializationSchema<TestKeyBean> {


    @Override
    public TestKeyBean deserialize(byte[] message) throws IOException {
        //解析二进制
        String body = new String(message, "UTF-8");
        JSONObject json = JSON.parseObject(body);
        if(json!=null){
            TestKeyBean testKeyBean=new TestKeyBean(json.get("record").toString());
            return testKeyBean;
        }
        return null;
    }

    @Override
    public boolean isEndOfStream(TestKeyBean testKeyBean) {
        return false;
    }

    @Override
    public TypeInformation<TestKeyBean> getProducedType() {
        return TypeInformation.of(TestKeyBean.class);
    }
}
