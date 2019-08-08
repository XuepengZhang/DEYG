package com.taobao.yugong.toCache.kafka;

import com.alibaba.fastjson.JSON;
import com.taobao.yugong.common.model.record.OracleIncrementRecord;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class OracleIncrementRecordDeserializer implements Deserializer<OracleIncrementRecord> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public OracleIncrementRecord deserialize(String topic, byte[] data) {
        return JSON.parseObject(data, OracleIncrementRecord.class);
    }

    @Override
    public void close() {

    }
}
