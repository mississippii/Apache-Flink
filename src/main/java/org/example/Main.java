package org.example;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

public class Main {
    public static void main(String[] args) throws Exception {
        Thread producerThread = new Thread(() -> {
            WordProducer producer = WordProducer.getProducerObj();
            producer.startWordProduce();
        });
        producerThread.start();
        FlinkPipeline.createPipeline();

        try {
            producerThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}