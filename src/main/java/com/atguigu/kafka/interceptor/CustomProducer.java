package com.atguigu.kafka.interceptor;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class CustomProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        // Kafka服务端的主机名和端口号
        props.put("bootstrap.servers", "linux01:9092");
        // 等待所有副本节点的应答
        props.put("acks", "all");
        // 消息发送最大尝试次数
        props.put("retries", 0);
        // 一批消息处理大小
        props.put("batch.size", 16384);
        // 请求延时
        props.put("linger.ms", 1);
        // 发送缓存区内存大小
        props.put("buffer.memory", 33554432);
        // key序列化
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // value序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //为Producer注册拦截器
        List<Object> inList = new ArrayList<>();
        inList.add("com.atguigu.kafka.interceptor.TimeInterceptor");
        inList.add("com.atguigu.kafka.interceptor.CounterInterceptor");
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, inList);

        //实例化KafkaProducer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        for (int i = 0; i < 50; i++) {
            producer.send(new ProducerRecord<String, String>("test1", Integer.toString(i), "hello world-" + i));
        }

        producer.close();
    }
}

