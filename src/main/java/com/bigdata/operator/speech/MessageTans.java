package com.bigdata.operator.speech;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import com.bigdata.bean.GroupSpeechInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wangtan
 * @Date 2021/1/14
 */
public class MessageTans implements MapFunction<String, GroupSpeechInfo> {
    private static final Logger logger = LoggerFactory.getLogger(MessageTans.class);

    /**
     * 将String类型的输入转化成一个对象返回
     *
     * @param message
     * @return
     * @throws Exception
     */
    @Override
    public GroupSpeechInfo map(String message) throws Exception {
        if (StringUtils.isBlank(message)) {
            return new GroupSpeechInfo();
        }
        JSONObject jsonObject = JSON.parseObject(message);
        GroupSpeechInfo result = JSONObject.parseObject(jsonObject.toJSONString(), GroupSpeechInfo.class);
        return result;
    }
}
