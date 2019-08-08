package com.taobao.yugong.toCache;

import com.taobao.yugong.common.model.record.IncrementRecord;
import com.taobao.yugong.common.model.record.Record;
import com.taobao.yugong.common.utils.YuGongUtils;
import com.taobao.yugong.exception.YuGongException;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class RecordCacheApplierImpl implements RecordCacheApplier {

    private final Logger logger = LoggerFactory.getLogger(RecordCacheApplierImpl.class);
    /** 生产者 */
    private Producer<String, Record> producerFull;
    private Producer<String, Record> producerInc;
    private String server;
    private String topic;
    private String fullStr = "-full";
    private String incStr = "-inc";
    private Integer type = 1;

    public RecordCacheApplierImpl(String server, String topic) {
        this.server = server;
        this.topic = topic;
        initKafka();
    }

    public void applierCache(List<Record> records) throws YuGongException {
        if (YuGongUtils.isEmpty(records)) {
            return;
        }
        if (!isInc(records)) {
            for (Record record : records) {
                producerSendFull(record);
            }
        } else {
            for (Record record : records) {
                producerSendInc(record);
            }
        }
    }

    private void initKafka(){
        producerFull = getProducerFull(server);
        producerInc = getProducerInc(server);
    }

    /**
     * 生产消息
     * @param value
     */
    public void producerSendFull(final Record value){
        logger.info("record:{}",value.toString());
        try {
            producerFull.send(new ProducerRecord<String, Record>(topic+fullStr, value),
                    new Callback() {
                        public void onCompletion(RecordMetadata metadata, Exception e) {
                            if (e != null){
                                Long offset_temp = null;
                                if(metadata!=null){
                                    offset_temp = metadata.offset();
                                }
                                logger.error(e.getMessage() + ",value is: "+value+ ",offset is: "+offset_temp,e);
                            }
                        }
                    });
        }catch (Exception e){
            logger.error(e.getMessage(),e);
            // 异常时重连
            initKafka();
        }
    }

    public void producerSendInc(final Record value){
        logger.info("record:{}",value.toString());
        try {
            producerInc.send(new ProducerRecord<String, Record>(topic+incStr, value),
                    new Callback() {
                        public void onCompletion(RecordMetadata metadata, Exception e) {
                            if (e != null){
                                Long offset_temp = null;
                                if(metadata!=null){
                                    offset_temp = metadata.offset();
                                }
                                logger.error(e.getMessage() + ",value is: "+value+ ",offset is: "+offset_temp,e);
                            }
                        }
                    });
        }catch (Exception e){
            logger.error(e.getMessage(),e);
            // 异常时重连
            initKafka();
        }
    }

    /**
     * 获得kafka生产者
     * @return
     */
    public static KafkaProducer getProducerFull(String servers){
        Properties props = new Properties();
        props.put("bootstrap.servers", servers);// 因为响应内容带有主机名，本地需要配置hosts映射
        //0代表：不进行消息接收是否成功的确认(默认值)；
        //1代表：当Leader副本接收成功后，返回接收成功确认信息；
        //-1(all)代表：当Leader和Follower副本都接收成功后，返回接收成功确认信息；
        props.put("acks", "all");
        props.put("retries", 0);// 重试次数，不重试防止重复数据发出
        props.put("batch.size", 3355443);// 每个分区未发送消息缓存的大小,默认时可立即发送，即便缓冲空间还没有满
        props.put("linger.ms", 1);// 等待请求毫秒数，希望更多的消息填补到未满的缓存（聚合再发送，提高吞吐量）
        props.put("buffer.memory", 33554432);// 缓存总量，当消息发送速度比传输到服务器的快，将会耗尽这个缓存。缓存空间耗尽，其他发送调用将被阻塞
        props.put("max.request.size", 15728640);
        //key和value对象ProducerRecord转换成字节
        props.put("max.block.ms", 9000);// 阻塞的阈值时间，超过TimeoutException
        props.put("compression.type", "lz4");// 压缩格式，默认不压缩，还可以为gzip, snappy, or lz4
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");// 生产序列化类型
        props.put("value.serializer", "com.taobao.yugong.toCache.kafka.RecordSerializer");
        return new KafkaProducer(props);
    }

    /**
     * 获得kafka生产者
     * @return
     */
    public static KafkaProducer getProducerInc(String servers){
        Properties props = new Properties();
        props.put("bootstrap.servers", servers);// 因为响应内容带有主机名，本地需要配置hosts映射
        //0代表：不进行消息接收是否成功的确认(默认值)；
        //1代表：当Leader副本接收成功后，返回接收成功确认信息；
        //-1(all)代表：当Leader和Follower副本都接收成功后，返回接收成功确认信息；
        props.put("acks", "all");
        props.put("retries", 0);// 重试次数，不重试防止重复数据发出
        props.put("batch.size", 3355443);// 每个分区未发送消息缓存的大小,默认时可立即发送，即便缓冲空间还没有满
        props.put("linger.ms", 1);// 等待请求毫秒数，希望更多的消息填补到未满的缓存（聚合再发送，提高吞吐量）
        props.put("buffer.memory", 33554432);// 缓存总量，当消息发送速度比传输到服务器的快，将会耗尽这个缓存。缓存空间耗尽，其他发送调用将被阻塞
        props.put("max.request.size", 15728640);
        //key和value对象ProducerRecord转换成字节
        props.put("max.block.ms", 9000);// 阻塞的阈值时间，超过TimeoutException
        props.put("compression.type", "lz4");// 压缩格式，默认不压缩，还可以为gzip, snappy, or lz4
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");// 生产序列化类型
        props.put("value.serializer", "com.taobao.yugong.toCache.kafka.OracleIncrementRecordSerializer");

        return new KafkaProducer(props);
    }

    private boolean isInc(List<Record> records) {
        return records.get(0) instanceof IncrementRecord;
    }
}
