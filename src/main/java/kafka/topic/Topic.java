package kafka.topic;

import kafka.admin.AdminUtils;
import kafka.admin.BrokerMetadata;
import kafka.server.ConfigType;
import kafka.utils.ZkUtils;
import org.apache.kafka.common.security.JaasUtils;
import scala.collection.Map;
import scala.collection.Seq;


import java.util.Properties;

/**
 * kafka主题操作
 */
public class Topic {

    //连接ZK
    private static final String ZK_CONNECT = "server-1:2181,server-2:2181,server-3:2181";
    //session过期时间
    private static final int SESSION_TIMEOUT = 30000;
    //连接超时时间
    private static final int CONNECT_TIMEOUT = 30000;

    /**
     * 新增主题
     * @param topic 主题名
     * @param partition 分区数
     * @param repilca 副本数
     * @param properties 主题级别配置
     */

    public static void createTopic(String topic, int partition, int repilca, Properties properties){
        ZkUtils zkUtils = null ;
        try{
            //实例化ZKUtil
            zkUtils = ZkUtils.apply(ZK_CONNECT,SESSION_TIMEOUT,CONNECT_TIMEOUT, JaasUtils.isZkSecurityEnabled());
            if(!AdminUtils.topicExists(zkUtils,topic)){//主题不存在创建
                AdminUtils.createTopic(zkUtils,topic,partition,repilca,properties,AdminUtils.createTopic$default$6());
            } else {
                //进行相应的处理
            }
        } catch (Exception e ){
            e.printStackTrace();
        } finally {
            zkUtils.close();
        }
    }

    /**
     * 修改主题配置
     * @param topic
     * @param properties
     */
    public static void modifyTopicConfig (String topic,Properties properties){
        ZkUtils zkUtils = null;
        try{
            //实例化ZKUtil
            zkUtils = ZkUtils.apply(ZK_CONNECT,SESSION_TIMEOUT,CONNECT_TIMEOUT, JaasUtils.isZkSecurityEnabled());
            //首先获取当前已经有的配置，这里是查询主题级别的配置，因此指定配置类型为topic
            Properties curProp = AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic(),topic);//
            curProp.putAll(properties);
            AdminUtils.changeTopicConfig(zkUtils,topic,curProp);
            //增加分区操作
            //3:1表示分区0对应的brokerId
            AdminUtils.addPartitions(zkUtils,"partiton-api-foo",2,"3:1,2:3" ,true,AdminUtils.addPartitions$default$6());
        } catch (Exception e ){
            e.printStackTrace();
        } finally {
            zkUtils.close();
        }
    }

    /**
     * 分区副本重新分配
     */
    public static void modifyPartition(){
        ZkUtils zkUtils = null;
        try{
            //1.实例化ZkUtils
            zkUtils = ZkUtils.apply(ZK_CONNECT,SESSION_TIMEOUT,CONNECT_TIMEOUT,JaasUtils.isZkSecurityEnabled());
            //2.获取代理元数据信息
            Seq<BrokerMetadata> brokerMeta = AdminUtils.getBrokerMetadatas(zkUtils,AdminUtils.getBrokerMetadatas$default$2(),AdminUtils.getBrokerMetadatas$default$3());
            //3.生成分区副本分配方案:2个分区、3个副本
            Map<Object, Seq<Object>> replicaAssign = AdminUtils.assignReplicasToBrokers(brokerMeta,2,3,AdminUtils.assignReplicasToBrokers$default$4(),AdminUtils.assignReplicasToBrokers$default$5());
            //4.修改分区副本分配方案
            AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils,"partition-api-foo",replicaAssign,null,true);
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            zkUtils.close();
        }

    }

    public static void delTopic(String topic){
        ZkUtils zkUtils = null;
        try {
            //1.实例化ZkUtils
            zkUtils = ZkUtils.apply(ZK_CONNECT,SESSION_TIMEOUT,CONNECT_TIMEOUT,JaasUtils.isZkSecurityEnabled());
            AdminUtils.deleteTopic(zkUtils,topic);
        } catch (Exception e ){
            e.printStackTrace();
        }
    }
}
