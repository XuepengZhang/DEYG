package com.taobao.yugong.toCache.kafka;

import com.alibaba.fastjson.JSON;
import com.taobao.yugong.common.model.record.Record;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class RecordDeserializer implements Deserializer<Record> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public Record deserialize(String topic, byte[] data) {
        return JSON.parseObject(data, Record.class);
    }

    @Override
    public void close() {

    }
}
