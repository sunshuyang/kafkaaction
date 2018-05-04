package kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * 自动提交偏移量的消费者实例
 */
public class QuotationConsumerAutoCommit {

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
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(initConfig());//创建消费者
        consumer.subscribe(Arrays.asList(TOPIC_NAME));//订阅主题
        try{
            while (true){
                //长轮询拉取消息
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String,String> record:records){
                    System.out.printf("partition = %d, offset = %d, key = %s, value = %s%n",
                            record.partition(),
                            record.offset(),
                            record.key(),
                            record.value()
                    );
                }
            }
        } catch (Exception e ){
            //异常处理
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }

}
