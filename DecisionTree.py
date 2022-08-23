package com.itheima;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaProducerUtil {
    public KafkaProducer<String,String> kafkaproducer=null;
    //构造一个kafka构造方法
    public KafkaProducerUtil(String brokerList){
        Properties prpo=new Properties();
        prpo.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,brokerList);
        prpo.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,String.class.getName());
        prpo.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,String.class.getName());
        this.kafkaproducer=new KafkaProducer<String, String>(prpo);
    }
public void close(){
        this.kafkaproducer.close();
}
    public static void main(String[] args) {
        //定义broker-list
        String brokerList="cluster0.hadoop:6667,cluster:5556";
        //定义topic
        String topic="TestKafka4Job017";
        //获取kafka producer工具类
        KafkaProducerUtil producer=new KafkaProducerUtil(brokerList);
        //发送消息到kafka集群
     producer.kafkaproducer.send(new ProducerRecord<String,String>(topic,"收复台湾"));
         producer.close();
         System.out.println("发送消息完成");
    }

}
