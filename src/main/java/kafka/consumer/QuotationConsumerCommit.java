package kafka.consumer;


import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * 手动提交偏移量的消费者实例
 */
public class QuotationConsumerCommit {


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
        config.put("fetch.max.bytes",1024);//为了便于测试，这里设置一次fetch请求取得的数据最大值为1kb，默认5Mb
        config.put("enable.auto.commit", false);//显示设置偏移量自动提交
        config.put("auto.commit.interval.ms", 1000);//设置偏移量提交时间间隔
        config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        config.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.putAll(config);
        return properties;
    }

    public static void main(String[] args) {
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(initConfig());
        //订阅主题
        consumer.subscribe(Arrays.asList(TOPIC_NAME));
        try {
            int minCommitSize = 10 ;//最少处理10条消息提交
            int icount = 0; //消息计算器
            while(true){
                //等待拉取消息
                ConsumerRecords<String ,String> records = consumer.poll(1000);
                for (ConsumerRecord<String,String> record:records){
                    //简单打印出消息内容，模拟业务处理
                    System.out.printf("partition = %d ,offset = %d ,key = %s ,value = %s",
                            record.partition(),
                            record.offset(),
                            record.key(),
                            record.value()
                    );
                    icount++;
                }
                //在业务成功处理后 提交偏移量
                if(icount >= minCommitSize){
                    consumer.commitAsync(new OffsetCommitCallback() {
                        @Override
                        public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
                            if(null == e){
                                System.out.println("提交成功");
                            } else{
                                System.out.println("提交失败");
                            }
                        }
                    });
                }
                icount = 0;
            }
        } catch (Exception e ){
            e.printStackTrace();
        } finally {
            consumer.close();
        }

    }
}
