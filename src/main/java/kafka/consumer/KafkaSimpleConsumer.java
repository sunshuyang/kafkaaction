package kafka.consumer;

import com.sun.deploy.util.StringUtils;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;

import kafka.message.MessageAndOffset;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;
import org.jboss.netty.util.internal.StringUtil;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


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

    /**
     * 获取分区元数据方法
     * @param brokerList
     * @param port
     * @param topic
     * @param partitionId
     * @return
     */
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

    /**
     * 获取消息偏移量方法
     * @param consumer
     * @param topic
     * @param partition
     * @param beginTime
     * @param clientName
     * @return
     */
    private long getLastOffset(SimpleConsumer consumer,String topic ,int partition,long beginTime,String clientName){
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic,partition);
        Map<TopicAndPartition,PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        //设置获取消息起始offsset
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(beginTime,1));
        //构造获取offset请求
        OffsetRequest request = new OffsetRequest(requestInfo,kafka.api.OffsetRequest.CurrentVersion(),clientName);
        OffsetResponse response = consumer.getOffsetsBefore(request);
        if(response.hasError()){
            LOG.error("Fetch last offset occurs exception:"+response.errorCode(topic,partition));
            return -1;
        }
        long[] offsets = response.offsets(topic,partition);
        if(null == offsets || offsets.length == 0){
            LOG.error("Fetch last offset occurs error,offsets is null .");
            return -1;
        }
        return offsets[0];
    }

    /**
     * 实现消费者拉取消息功能 主题名为stock-quotation 分区5
     * @param brokerList
     * @param port
     * @param topic
     * @param partitionId
     */
    public void consume(List<String> brokerList,int port,String topic,int partitionId){
        SimpleConsumer consumer = null;
        try{
            //1.首先获取指定分区的元数据信息
            PartitionMetadata metadata = fetchPartitionMetadata(brokerList,port,topic,partitionId);
            if(metadata == null){
                LOG.error("Can't find metadata info ");
                return ;
            }
            if(metadata.leader() == null){
                LOG.error("Can't find the partition:" + partitionId + "'s leader.");
                return ;
            }
            String leadBroker = metadata.leader().host();
            String clientId = "client-" + topic + "-" + partitionId;

            //2.创建一个消息者作为消费消息的真正执行者
            consumer = new SimpleConsumer(leadBroker,port,TIME_OUT,BUFFER_SIZE,clientId);
            //设置时间为kafka.api.OffsetRequest.EarliestTime()从最新消息起始处开始
            long lastOffset = getLastOffset(consumer,topic,partitionId,kafka.api.OffsetRequest.EarliestTime(),clientId);
            int errorNum = 0;
            FetchRequest fetchRequest = null;
            FetchResponse fetchResponse = null;
            while(lastOffset > -1){
                //当在循环过程中出错时将起始实例化consumer关闭并设置null
                if(consumer == null){
                    consumer = new SimpleConsumer(leadBroker, port,TIME_OUT,BUFFER_SIZE,clientId);
                }
                //3.构造获取消息request
                fetchRequest = new FetchRequestBuilder().clientId(clientId).addFetch(topic,partitionId,lastOffset,FETCH_SIZE).build();
                //4.获取响应并处理
                fetchResponse = consumer.fetch(fetchRequest);
                if (fetchResponse.hasError()) { // 若发生错误
                    errorNum++;
                    if (errorNum > MAX_ERROR_NUM){
                        break;
                    }
                    //获取错误码
                    short errorCode = fetchResponse.errorCode(topic,partitionId);
                    //offset已无效,因为在获取lastOffset时设置为从最早开始时间，若是这种错误码，
                    //我们再将时间设置为从LatestTime()开始查找
                    if(ErrorMapping.OffsetOutOfRangeCode() == errorCode){
                        lastOffset = getLastOffset(consumer,topic,partitionId,kafka.api.OffsetRequest.LatestTime(),clientId);
                        continue;
                    } else if (ErrorMapping.OffsetsLoadInProgressCode() == errorCode){
                        Thread.sleep(30000);//若是这种异常则让线程休眠
                        continue;
                    } else {//这里只是简单地关闭当前分区Leader信息实例化的Consumer,
                            //并没有对代理失效时进行相应的处理
                        consumer.close();
                        consumer = null;
                        continue;
                    }
                } else {
                    errorNum = 0;
                    long fetchNum = 0;
                    for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(topic,partitionId)){
                        long currentOffset = messageAndOffset.offset();
                        if(currentOffset < lastOffset){
                            LOG.error("Fetch an old offset:"+currentOffset+"expect the offset is greater than "+lastOffset);
                            continue;
                        }
                        lastOffset = messageAndOffset.nextOffset();
                        ByteBuffer payload = messageAndOffset.message().payload();

                        byte[] bytes = new byte[payload.limit()];
                        payload.get(bytes);
                        //简单打印出消息及消息offset
                        LOG.info("message:" + (new String(bytes,"UTF-8"))+",offset:"+messageAndOffset.offset());
                        fetchNum++;
                    }
                    //没有消息阻塞几秒
                    if(fetchNum == 0){
                        try{
                            Thread.sleep(1000);
                        } catch (InterruptedException ie){
                        }
                    }
                }
            }


        } catch (InterruptedException e) {
            LOG.error("Consume message occurs excepton.",e);
        } catch (UnsupportedEncodingException e) {
            LOG.error("Consume message occurs excepton.",e);
        } finally {
            if(null != consumer){
                consumer.close();
            }
        }
    }

    public static void main(String[] args) {
        KafkaSimpleConsumer consumer = new KafkaSimpleConsumer();
        consumer.consume(Arrays.asList(StringUtils.splitString(BROKER_LIST,",")),PORT,"stock-quotation-partition",5);
    }
}
