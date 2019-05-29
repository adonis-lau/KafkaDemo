package com.atguigu.kafka.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

public class CounterInterceptor implements ProducerInterceptor<String, String> {
    private long successCount = 0;
    private long errorCount = 0;

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        return producerRecord;
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        if (e == null) {
            successCount ++;
        } else {
            errorCount++;
        }
    }

    @Override
    public void close() {
        System.out.println("successCount = " + successCount);
        System.out.println("errorCount = " + errorCount);
    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
