package org.example;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Time;
import java.util.Date;
import java.util.Properties;

public class FlinkPipeline {

    private static final Logger logger = LoggerFactory.getLogger(FlinkPipeline.class);

    public static void createPipeline() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000);
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
        env.setStateBackend(new FsStateBackend("file:///tmp/flink-checkpoints"));
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "103.248.13.73:9092");
        kafkaProps.setProperty("group.id", "flink-consumer-group");

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>("flink1", new SimpleStringSchema(), kafkaProps);
        kafkaConsumer.setCommitOffsetsOnCheckpoints(true);

        DataStreamSource<String> words = env.addSource(kafkaConsumer);

        DataStream<String> processedWords = words
                .map(new WordProcessingMetric())
                .name("Word Processing Metric");

        DataStream<Tuple2<String, Integer>> wordLengths =processedWords
                .map(new RichMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String word) throws Exception {
                        String parallelism = String.valueOf(getRuntimeContext().getNumberOfParallelSubtasks());
                        String hostName = java.net.InetAddress.getLocalHost().getHostName();
                        long timestamp = System.currentTimeMillis();
                        String formattedTime = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date(timestamp));
                        logger.info("Timestamp: {}, TaskManager Host: {}, Parallelism: {}, Word Picked: {}", formattedTime, hostName, parallelism, word);

                        System.out.println(String.format("Timestamp: %s, TaskManager Host: %s, Parallelism: %s, Word Picked: %s",
                                formattedTime, hostName, parallelism, word));
                        return new Tuple2<>(word, word.length());
                    }
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .setParallelism(4);
        Thread.sleep(5000);
        wordLengths.addSink( new MySQLSink());

        env.execute("Flink Word Count with Metrics");
    }

}

