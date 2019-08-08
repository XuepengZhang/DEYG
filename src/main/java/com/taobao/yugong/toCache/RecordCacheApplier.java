package com.taobao.yugong.toCache;

import com.taobao.yugong.common.model.record.Record;
import com.taobao.yugong.exception.YuGongException;

import java.util.List;

public interface RecordCacheApplier {
    public void applierCache(List<Record> records) throws YuGongException;
}
