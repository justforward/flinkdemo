package com.bigdata.bean;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.Data;

import java.util.List;

/**
 * @Auther wangtan
 * @Date 2020/10/16
 */
@Data
public class CycleTagBean {
    private Integer bizType;
    private Integer cityId;
    private Long vehicleId;
    private long eventTime;
    private Integer opStatus;
    private List<Integer> allTagIds;
    private Integer tagId;
    private String tagName;
    private Integer tagType;
    private Integer eventType;

    //为了序列化，存放序列化信息 将整个对象输出使用
    private JSONObject content;
}
