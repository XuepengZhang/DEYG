package com.taobao.yugong.toCache.kafka;

import com.alibaba.fastjson.JSON;
import com.taobao.yugong.common.model.record.OracleIncrementRecord;
import org.apache.kafka.common.serialization.Serializer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;

public class OracleIncrementRecordSerializer implements Serializer<OracleIncrementRecord> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, OracleIncrementRecord data) {
        String value = JSON.toJSONString(data);
//        OracleIncrementRecord o = (OracleIncrementRecord)JSON.parseObject(value,OracleIncrementRecord.class);
//        OracleIncrementRecord record = (OracleIncrementRecord)o;
//        System.out.println(record);
        return JSON.toJSONBytes(data);
//        String s= JSON.toJSONStringWithDateFormat(data,"yyyy-MM-dd HH:mm:ss");
//        return null;
    }

    @Override
    public void close() {

    }
}
