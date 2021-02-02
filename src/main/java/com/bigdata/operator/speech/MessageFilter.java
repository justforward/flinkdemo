package com.bigdata.operator.speech;


import com.bigdata.bean.GroupSpeechInfo;
import com.bigdata.utils.DayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;

/**
 * @author wangtan
 * @Date 2021/1/14
 */
public class MessageFilter implements FilterFunction<GroupSpeechInfo> {

    @Override
    public boolean filter(GroupSpeechInfo groupSpeechInfo) throws Exception {
        if (StringUtils.isNotBlank(groupSpeechInfo.getHandleResult()) && groupSpeechInfo.getHandleResult().equals("success")) {
            if (StringUtils.isNotBlank(groupSpeechInfo.getMsgType())&& groupSpeechInfo.getMsgType().equals("5003")) {
                if (StringUtils.isNotBlank(groupSpeechInfo.getChatroomId()) && groupSpeechInfo.getCreateTime()!=null) {
                    if(groupSpeechInfo.getCreateTime()> DayUtils.todayTimeStamp()){
                        return true;
                    }
                }
            }
        }
        return false;
    }
}
