package com.bigdata.serializer;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.bigdata.bean.CycleTagBean;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @Auther wangtan
 * @Date 2020/10/16
 */
public class HmCycleTagSchema implements DeserializationSchema<CycleTagBean> {
    @Override
    public CycleTagBean deserialize(byte[] message) throws IOException {
        String body = new String(message, "UTF-8");
        JSONObject json = JSON.parseObject(body);
        if(json!=null){
            CycleTagBean cycleTag=new CycleTagBean();
            cycleTag.setBizType(json.getInteger("bizType"));
            cycleTag.setCityId(json.getInteger("cityId"));
            cycleTag.setVehicleId(json.getLong("vehicleId"));
            cycleTag.setEventTime(json.getLong("eventTime"));
            cycleTag.setOpStatus(json.getInteger("opStatus"));

            JSONArray allTagIds = json.getJSONArray("allTagIds");
            if(allTagIds!=null&&!allTagIds.isEmpty()){
                List<Integer> allTagIdList=new ArrayList<>();
                for(Object obj:allTagIds){
                    Integer tag = (Integer) obj;
                    allTagIdList.add(tag);
                }
                cycleTag.setAllTagIds(allTagIdList);
            }
            cycleTag.setTagId(json.getInteger("tagId"));
            cycleTag.setTagName(json.getString("tagName"));
            cycleTag.setTagType(json.getInteger("tagType"));
            cycleTag.setEventType(json.getInteger("eventType"));
            return cycleTag;
        }
        return null;
    }

    @Override
    public boolean isEndOfStream(CycleTagBean cycleTag) {
        return false;
    }

    @Override
    public TypeInformation<CycleTagBean> getProducedType() {
        return TypeInformation.of(CycleTagBean.class);
    }
}

