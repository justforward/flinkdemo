package com.bigdata.serializer;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

/**
 * @author wangtan
 * @Date 2021/3/11
 */
public class Test implements KeyedDeserializationSchema {


    @Override
    public Object deserialize(byte[] bytes, byte[] bytes1, String s, int i, long l) throws IOException {
        return null;
    }

    @Override
    public boolean isEndOfStream(Object o) {
        return false;
    }

    @Override
    public TypeInformation getProducedType() {
        return null;
    }
}
