package com.bigdata.bean;

import com.google.common.base.Objects;

/**
 * @author wangtan
 * @Date 2021/3/12
 */
public class TestKeyBean {
    private String record;

    public TestKeyBean(String record) {
        this.record = record;
    }

    @Override
    public int hashCode() {
        final int prime=31;
        int result=1;
        result=prime*result+((record==null)?0:record.hashCode());
        return result;
    }

    public String getRecord() {
        return record;
    }

    public void setRecord(String record) {
        this.record = record;
    }
}
