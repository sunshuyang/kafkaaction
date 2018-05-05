{
    "namespace":"com.kafka.serializer",
    "type":record",
    "name":"AvroStockQuotation",
    "fields":[
        {"name":"stockCode","type":"string"},
        {"name":"stockName","type":"string"},
        {"name":"tradeTime","tmvn ype":"long"},
        {"name":"preClosePrice","type":"float"},
        {"name":"openPrice","type":"float"},
        {"name":"currentPrice","type":"float"},
        {"name":"highPrice","type":"float"},
        {"name":"lowPrice","type":"float"},
    ]
}