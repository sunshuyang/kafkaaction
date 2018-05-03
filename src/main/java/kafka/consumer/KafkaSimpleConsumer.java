package kafka.consumer;

import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;

import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.List;


/**
 * 旧版本消费者API应用
 */

public class KafkaSimpleConsumer {
    private static final Logger LOG = Logger.getLogger(KafkaSimpleConsumer.class);
    /**
     * 指定Kafka集群代理列表，列表无需指定所有的代理地址
     * 只要保证能连上kafka即可，一般建议多个节点时至少写两个节点信息
     */
    private static final String BROKER_LIST = "172.117.12.61,172.117.12.62";
    //连接超时时间设置为1分钟
    private static final int TIME_OUT = 60*1000;
    //设置读取消息缓冲区大小
    private static final int BUFFER_SIZE = 1024*1024;
    //设置每次获得消息的条数
    private static final int FETCH_SIZE = 100000;
    //broker的端口
    private static final int PORT = 9092;
    //设置容忍发生错误是重试的最大次数
    private static final int MAX_ERROR_NUM = 3;


    private PartitionMetadata fetchPartitionMetadata(List<String> brokerList, int port, String topic, int partitionId){
        SimpleConsumer consumer = null; //获取元数据信息执行者
        TopicMetadataRequest metadataRequest = null;
        TopicMetadataResponse metadataResponse = null;
        List<TopicMetadata> topicMetadata = null; //元数据信息列表
        try{
            for(String host:brokerList){ //请求多个代理点
                //1.构造一个消费者用于获取元数据信息的执行者
                consumer = new SimpleConsumer(host,port,TIME_OUT,BUFFER_SIZE,"fetch-metadata");
                //2.构造请求主题的元数据的request
                metadataRequest = new TopicMetadataRequest(Arrays.asList(topic));
                //3.发送获取主题元数据的请求
                try{
                    metadataResponse = consumer.send(metadataRequest);
                } catch (Exception e){
                    LOG.error("Send opticMetadataRequest occurs exception.",e);
                    continue;
                }
                //4.获取主题元数据列表
                topicMetadata = metadataResponse.topicsMetadata();
                //5.主题元数据列表中提取指定的分区元数据信息
                for (TopicMetadata metadata : topicMetadata){
                    for (PartitionMetadata item:metadata.partitionsMetadata()){
                        if(item.partitionId() != partitionId){
                            continue;
                        } else {
                            return item;
                        }
                    }
                }

            }
        } catch (Exception e ){
            LOG.error("Fetch PartitionMetadata occurs exception",e);
        }finally {
            if(null != consumer){
                consumer.close();
            }
        }
        return null;
    }
}
