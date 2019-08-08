package com.taobao.yugong.toCache.kafka;

import com.alibaba.fastjson.JSON;
import com.taobao.yugong.common.model.record.IncrementRecord;
import com.taobao.yugong.common.model.record.OracleIncrementRecord;
import com.taobao.yugong.common.model.record.Record;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;

public class RecordSerializer implements Serializer<Record> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, Record data) {
        return JSON.toJSONBytes(data);
    }

    @Override
    public void close() {

    }
}
