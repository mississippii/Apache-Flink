package org.example;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class FlinkPipeline {

    public static void createPipeline() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "localhost:9092");
        kafkaProps.setProperty("group.id", "flink-consumer-group");

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>("flink1", new SimpleStringSchema(), kafkaProps);

        DataStream<String> words = env.addSource(kafkaConsumer);

        DataStream<Tuple2<String, Integer>> wordLengths = words.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String word) {
                return new Tuple2<>(word, word.length());
            }
        });
        wordLengths
                .map(tuple -> tuple.f0)
                .print("Consumed BY Flink");
        wordLengths.addSink(new MySQLSink());

        env.execute("Flink Word Count");
    }

}

