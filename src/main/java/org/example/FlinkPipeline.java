package org.example;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class FlinkPipeline {

    private static final Logger logger = LoggerFactory.getLogger(FlinkPipeline.class);

    public void createPipeline() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.enableCheckpointing(6000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(5 * 60 * 1000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000);
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                5,
                Time.of(10, TimeUnit.SECONDS)
        ));
        env.setStateBackend(new FsStateBackend("file:///tmp/flink-checkpoints"));
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "103.248.13.73:9092");
        kafkaProps.setProperty("group.id", "flink-consumer-group");

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>("flink1", new SimpleStringSchema(), kafkaProps);
        kafkaConsumer.setCommitOffsetsOnCheckpoints(true);

        DataStreamSource<String> words = env.addSource(kafkaConsumer).setParallelism(4);


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

        final StreamingFileSink<String> fileSink = StreamingFileSink
                .forRowFormat(new Path("/home/veer/Downloads/output"), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .build())
                .build();
        wordLengths
                .map(tuple -> tuple.f0 + " -> Length: " + tuple.f1)
                .returns(Types.STRING)
                .addSink(fileSink)
                .name("FileSink");

        env.execute("Bulk_SMS");
    }
}


