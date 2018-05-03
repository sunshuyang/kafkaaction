package kafka.producer;

import entity.StockQuotationInfo;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;

import java.text.DecimalFormat;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


/**
 * 多线程生产者
 */

public class KafkaProducerThread implements Runnable {
    private static final Logger LOG = Logger.getLogger(QuotationProducer.class);
    //设置实例生产消息的总数
    private static final int MSG_SIZE = 100;
    //线程数
    private static final int THREADS_NUMS = 10;
    //主题名称
    private static final String TOPIC = "stock-quotation";
    //kafka集群
    private static final String BROKER_LIST = "server-1:9092,server-2:9092,server-3:9092";
    private KafkaProducer<String,String> producer = null;
    private ProducerRecord<String,String> record = null;


    public KafkaProducerThread(KafkaProducer<String,String> producer,ProducerRecord<String,String> record){
        this.producer = producer;
        this.record = record;
    }

    /**
     * 1.初始化Kafka配置
     * @return
     */
    private static Properties initConfig(){
        Properties properties = new Properties();
        //Kafka broker列表
        properties.put(org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,BROKER_LIST);
        //设置序列化
        properties.put(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        return properties;
    }

    /**
     * 生产股票行情信息
     * @return
     */
    private static StockQuotationInfo createQuotationInfo(){
        StockQuotationInfo quotationInfo = new StockQuotationInfo();
        //随机产生1到10之间的整数，然后与600100想加组成股票代码
        Random r = new Random();
        Integer stockCode = 600100 + r.nextInt();
        //随机产生一个0到1的的整浮点数
        float random = (float) Math.random();
        //设置涨幅规则
        if(random/2<0.5){
            random = -random;
        }
        DecimalFormat decimalFormat = new DecimalFormat(".00");//保存两位有效数字
        quotationInfo.setCurrentPrice(Float.valueOf(decimalFormat.format(11+random)));
        quotationInfo.setPreClosePrice(11.80f);//设置昨日开盘为固定值
        quotationInfo.setOpenPrice(11.50f);//设置开盘价格
        quotationInfo.setLowPrice(10.5f);//设置最低价，并不考虑10%限制 //以及当时是否是最低价
        quotationInfo.setHighPrice(12.5f);//设置最高价，并不考虑10%限制
        quotationInfo.setStockCode(stockCode.toString());
        quotationInfo.setTradeTime(System.currentTimeMillis());
        quotationInfo.setStockName("股票-"+stockCode);
        return quotationInfo;
    }



    public void run() {
        producer.send(record, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if(null != e){
                    LOG.error("Send message occurs exception.",e);
                }
                if(null != recordMetadata){
                    LOG.info(String.format("offset:%s,partition:%s",recordMetadata.offset(),recordMetadata.partition()));
                }
            }
        });
    }

    public static void main(String[] args) {
        ProducerRecord<String,String> record = null;
        StockQuotationInfo quotationInfo = null;
        Properties configs = initConfig();
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(configs);
        ExecutorService executor = Executors.newFixedThreadPool(THREADS_NUMS) ;
        try{
            for (int i = 0; i < MSG_SIZE; i++) {
                quotationInfo = createQuotationInfo();
                //消息对象
                record = new ProducerRecord<String, String>(TOPIC,null,
                        quotationInfo.getTradeTime(),
                        quotationInfo.getStockCode(), quotationInfo.toString());
                //发送消息时指定一个偏Callback，实现onCompletion()方法，在成功发送后获取消息偏移量和分区
                executor.submit(new KafkaProducerThread(producer,record));
            }
        } catch (Exception e){
            LOG.error("Send Message occurs exception",e);
        } finally {
            producer.close();
        }

    }

}
