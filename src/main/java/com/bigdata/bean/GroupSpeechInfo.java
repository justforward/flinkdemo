package com.bigdata.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author wangtan
 * @Date 2021/2/2
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class GroupSpeechInfo {
    /**
     * 群Id
     */
    private String chatroomId;

    /**
     * 返回的结果：init 和Success两种
     */
    private String handleResult;

    /**
     * 回调类型码，5003代表这个是群内实时消息：有人发言
     * 该字段有可能不存在：当返回结果是init的时候该字段不存在
     */
    private String msgType;

    /**
     * 发言时间：发言时间是按照第三方回调的，消息的延迟？
     */
    private Long createTime;

}

