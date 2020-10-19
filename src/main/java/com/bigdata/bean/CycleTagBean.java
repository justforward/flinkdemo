package com.bigdata.bean;

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
}
