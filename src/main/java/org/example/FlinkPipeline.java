package org.example;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class FlinkPipeline {

    private static final Logger logger = LoggerFactory.getLogger(Pipeline.class);

    public void createPipeline() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000);
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
        env.setStateBackend(new FsStateBackend("file:///tmp/flink-checkpoints"));
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "192.168.0.32:8097");
        kafkaProps.setProperty("group.id", "flink-consumer-group");

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>("bulksms", new SimpleStringSchema(), kafkaProps);
        kafkaConsumer.setCommitOffsetsOnCheckpoints(true);

        DataStreamSource<String> words = env.addSource(kafkaConsumer);


        DataStream<Tuple2<String, Integer>> wordLengths =words
                .map(new RichMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String word) throws Exception {
                        try {
                            String parallelism = String.valueOf(getRuntimeContext().getNumberOfParallelSubtasks());
                            String hostName = java.net.InetAddress.getLocalHost().getHostAddress();
                            long timestamp = System.currentTimeMillis();
                            String formattedTime = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date(timestamp));
                            logger.info("Timestamp: {}, TaskManager Host: {}, Parallelism: {}, Word Picked: {}", formattedTime, hostName, parallelism, word);
                            return new Tuple2<>(word, word.length());
                        } catch (Exception e) {
                            logger.error("Error processing word: {}", word, e);
                            throw e;
                        }
                    }
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .setParallelism(4);

        wordLengths
                .map(tuple -> tuple.f0+" ")
                .returns(Types.STRING)
                .addSink( new RestSink())
                .name("RestSink");

        env.execute("Flink Word Count with Rest");
    }
}


