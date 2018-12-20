package com.kafka.producer;

import com.kafka.entity.StockQuotationInfo;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;

import java.text.DecimalFormat;
import java.util.Properties;
import java.util.Random;


/**
 * 单线程生产者
 */

public class QuotationProducer {
    private static final Logger LOG = Logger.getLogger(QuotationProducer.class);
    //设置实例生产消息的总数
    private static final int MSG_SIZE = 100;
    //主题名称
    private static final String TOPIC = "stock-quotation";
    //kafka集群
    private static final String BROKER_LIST = "server-1:9092,server-2:9092,server-3:9092";
    private static KafkaProducer<String,String> producer = null;

    static{
        //1.构造用于实例化KafkaProducer的Properties信息
        Properties configs = initConfig();
        //2.初始化一个KafkaProducer
        producer = new KafkaProducer<String, String>(configs);
    }

    /**
     * 1.初始化Kafka配置
     * @return
     */
    static Properties initConfig(){
        Properties properties = new Properties();
        //Kafka broker列表
        properties.put(org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,BROKER_LIST);
        //设置序列化
        properties.put(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        return properties;
    }

    public static void main(String[] args) {
        ProducerRecord<String,String> record = null;
        StockQuotationInfo quotationInfo = null;
        try{
            int num = 0;
            for (int i = 0; i < MSG_SIZE; i++) {
                quotationInfo = KafkaProducerThread.createQuotationInfo();
                //消息对象
                //默认分区策略，同一支股票发送到同一个分区下
                record = new ProducerRecord<String, String>(TOPIC,null,quotationInfo.getTradeTime(),
                        quotationInfo.getStockCode(),quotationInfo.toString());
                //发送消息时指定一个偏Callback，实现onCompletion()方法，在成功发送后获取消息偏移量和分区
                KafkaProducerThread.sendMsgFunc(producer, record, LOG);
                if (num++ % 10 ==0){
                    Thread.sleep(2000L);//休眠2s
                }
            }
        } catch (Exception e){
            LOG.error("Send Message occurs exception",e);
        } finally {
            producer.close();
        }

    }
}
