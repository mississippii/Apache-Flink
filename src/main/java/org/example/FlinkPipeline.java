package org.example;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class FlinkPipeline {

    public static void createPipeline() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000);
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
        env.setStateBackend(new FsStateBackend("file:///tmp/flink/checkpoints"));
        env.getCheckpointConfig().setCheckpointTimeout(60000); // Timeout for checkpointing in ms
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1); // Maximum concurrent checkpoints
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "localhost:9092");
        kafkaProps.setProperty("group.id", "flink-consumer-group");

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>("flink1", new SimpleStringSchema(), kafkaProps);
        kafkaConsumer.setCommitOffsetsOnCheckpoints(true);

        DataStreamSource<String> words = env.addSource(kafkaConsumer);
        words.setParallelism(4);

        DataStream<Tuple2<String, Integer>> wordLengths = words
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String word) {
                return new Tuple2<>(word, word.length());
            }
                })
                .setParallelism(4);

        String outputPath = "/opt/flink-1.20.0/examples/jar/word_counts/";
        wordLengths
                .map(tuple -> tuple.f0 + ", " + tuple.f1) // Convert Tuple2 to a String format
                .writeAsText(outputPath, FileSystem.WriteMode.OVERWRITE);
        wordLengths
                .map(tuple -> tuple.f0)
                .print("Consumed BY Flink");
//        wordLengths.addSink(new MySQLSink());

        env.execute("Flink Word Count");
    }

}

