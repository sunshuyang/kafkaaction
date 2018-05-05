package com.kafka.serializer;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

public class AvroSerializer <T extends SpecificRecordBase> implements Serializer<T> {
    @Override
    public void configure(Map map, boolean b) {

    }

    /**
     * 实现序列化方法
     * @param topic
     * @param data
     * @return
     */
    @Override
    public byte[] serialize(String topic, T data) {
        if(null == data){
            return null;
        }
        DatumWriter<T> writer = new SpecificDatumWriter<>(data.getSchema());
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        BinaryEncoder binaryEncoder = EncoderFactory.get().directBinaryEncoder(outputStream,null);
        try{
            writer.write(data,binaryEncoder);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return outputStream.toByteArray();
    }

    @Override
    public void close() {

    }
}
