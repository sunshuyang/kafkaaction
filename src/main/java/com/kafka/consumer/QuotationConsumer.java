package com.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.*;


/**
 * 新版消费者单线程
 */
public class QuotationConsumer {

    private static final long CONSUMER_TIME = 100;

    private static final String TOPIC_NAME = "stock-quotation";
    /**
     * 1.初始化consumer配置
     *
     * @return
     */
    private static Properties initConfig() {
        Properties properties = new Properties();
        Map<String, Object> config = new HashMap<String, Object>();
        //Kafka broker列表
        config.put("bootstrap.servers", "localhost:9092");
        config.put("group.id", "test");
        config.put("enable.auto.commit", true);//显示设置偏移量自动提交
        config.put("auto.commit.interval.ms", 1000);//设置偏移量提交时间间隔
        config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        config.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.putAll(config);
        return properties;
    }

    public static void main(String[] args) {
        Properties props = initConfig();
        // 创建KafkaConsumer对象
        final KafkaConsumer<String,String> consumer = new KafkaConsumer<>(props);
        // 消费者平衡回调监听器
        // 使用subscribe方法可以具有消费者自动负载均衡功能
        consumer.subscribe(Arrays.asList("stock-quotation"), new ConsumerRebalanceListener() {
            //消费者平衡操作开始之前，消费者停止拉取消息之后被调用
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                consumer.commitAsync();//提交偏移量
            }
            //消费者平衡之后，消费者开始拉取消息之前被调用
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                long committedOffset = -1;
                for (TopicPartition topicPartition : partitions){
                    //获取分区已消费的偏移量
                    committedOffset = consumer.committed(topicPartition).offset();
                    //重置偏移量到上一次提交的偏移量下一个位置处开始消费
                    consumer.seek(topicPartition,committedOffset+1);
                }
            }
        });

        // 订阅特定分区 分区0 和 分区2
        consumer.assign(Arrays.asList(new TopicPartition(TOPIC_NAME,0),new TopicPartition(TOPIC_NAME,2)));

        // 通过poll方法获取主题数据
        //consumer.poll(CONSUMER_TIME);


    }

}
