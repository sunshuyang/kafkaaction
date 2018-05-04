<h1>kafka入门与实践代码实现</h1>

<h2>主题管理API</h2>

kafka.topic.Topic ---- kafka主题操作  

<h2>生产者API</h2>

kafka.producer.QuotationProducer ---- 单线程生产者  
kafka.producer.KafkaProducerThread ---- 多线程生产者

<h2>消费者API</h2>

kafka.consumer.KafkaSimpleConsumer ---- 旧版本消费者API应用  
kafka.consumer.QuotationConsumer ---- 新版 消费者单线程  
kafka.consumer.QuotationConsumerAutoCommit ---- 新版 自动提交偏移量的消费者实例  
kafka.consumer.QuotationConsumerByDate ---- 新版 以时间戳查询消息  
kafka.consumer.QuotationConsumerCommit ---- 新版 手动提交偏移量的消费者实例  
Kafka.consumer.KafkaConsumerThread ---- 新版 多线程消费者
