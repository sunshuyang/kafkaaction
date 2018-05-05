package com.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * 以时间戳查询消息
 * 如果查询分区不存在，offsetsForTimes方法会阻塞
 */
public class QuotationConsumerByDate {


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
        config.put("client.id", "test");
        config.put("enable.auto.commit", true);//显示设置偏移量自动提交
        config.put("auto.commit.interval.ms", 1000);//设置偏移量提交时间间隔
        config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        config.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.putAll(config);
        return properties;
    }

    public static void main(String[] args) {
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(initConfig());
        try {
            Map<TopicPartition,Long> timestampsToSearch = new HashMap<>();

            //0分区
            TopicPartition partition = new TopicPartition(TOPIC_NAME,0);
            // 设置查询12小时之前消息的偏移量
            timestampsToSearch.put(partition,(System.currentTimeMillis() - 12 * 3600 * 1000));
            //会返回时间大于等于查找时间的第一个偏移量
            Map<TopicPartition,OffsetAndTimestamp> offsetMap = consumer.offsetsForTimes(timestampsToSearch);
            OffsetAndTimestamp offsetTimestamp = null;
            //这里依然用for轮询，当然由于本例是查询一个分区，因此也可以用if处理
            for (Map.Entry<TopicPartition,OffsetAndTimestamp> entry:offsetMap.entrySet()){
                //若查询时间大于时间戳索引文件中的最大记录搜索时间，
                //此时value为空，即待查询时间点后没有新消息生成
                offsetTimestamp = entry.getValue();
                if(null != offsetTimestamp){
                    //重置消费起始偏移量
                    consumer.seek(partition,entry.getValue().offset());
                }
            }
            while(true){
                //等待拉取消息
                ConsumerRecords<String ,String> records = consumer.poll(1000);
                for (ConsumerRecord<String, String> record:records){
                    //简单打印出消息
                    System.out.printf("partition = %d, offset = %d, key = s%, value = s%n%",
                            record.partition(),
                            record.offset(),
                            record.key(),
                            record.value()
                    );

                }
            }
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            consumer.close();
        }

        //消费速度控制
        //consumer.pause();
        //consumer.resume();
    }
}
