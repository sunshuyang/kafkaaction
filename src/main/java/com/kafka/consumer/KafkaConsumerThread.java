package com.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

/**
 * 新版 多线程消费者
 */
public class KafkaConsumerThread extends Thread {
    //每个线程拥有私有的KafkaComsumer实例
    private KafkaConsumer<String, String> consumer;
    public KafkaConsumerThread(Map<String,Object> consumerCofig ,String topic){
        Properties props = new Properties();
        props.putAll(consumerCofig);
        this.consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList(topic));
    }

    @Override
    public void run(){
        try {
            while(true){
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String,String> record:records){
                    //简单打印出消息内容
                    System.out.printf("threadId = %s,partition = %d,offset = %d,key = %s value = %s%n"
                            ,Thread.currentThread().getId()
                            ,record.partition()
                            ,record.offset()
                            ,record.key()
                            ,record.value());
                }
            }
        } catch (Exception e ) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }

}
