<h1>kafka入门与实践代码实现</h1>

<h2>主题管理操作</h2>

com.kafka.topic.Topic ---- kafka主题操作  

<h2>生产者操作</h2>

com.kafka.producer.QuotationProducer ---- 单线程生产者  
com.kafka.producer.KafkaProducerThread ---- 多线程生产者

<h2>消费者操作</h2>

com.kafka.consumer.KafkaSimpleConsumer ---- 旧版本消费者API应用  
com.kafka.consumer.QuotationConsumer ---- 新版 消费者单线程  
com.kafka.consumer.QuotationConsumerAutoCommit ---- 新版 自动提交偏移量的消费者实例  
com.kafka.consumer.QuotationConsumerByDate ---- 新版 以时间戳查询消息  
com.kafka.consumer.QuotationConsumerCommit ---- 新版 手动提交偏移量的消费者实例  
com.kafka.consumer.KafkaConsumerThread ---- 新版 多线程消费者

<h2>序列化</h2>
com.kafka.serializer.AvroSerializer ---- 实现序列化方法（待完善）