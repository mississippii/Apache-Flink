package org.example;
import java.util.Properties;

public class Main {
    public static void main(String[] args) throws Exception {
        try {
            Thread producerThread = new Thread(() -> {
                try {
                    WordProducer producer = WordProducer.getProducerObj();
                    producer.startWordProduce();
                } catch (Exception e) {
                    throw new RuntimeException("Error starting WordProducer", e);
                }
            });
            Thread pipelineThread = new Thread(() -> {
                try {
                    FlinkPipeline pipeline = new FlinkPipeline();
                    pipeline.createPipeline();
                } catch (Exception e) {
                    throw new RuntimeException("Error starting FlinkPipeline", e);
                }
            });
            producerThread.start();
            pipelineThread.start();
            producerThread.join();
            pipelineThread.join();
        } catch (Exception e) {
            throw new RuntimeException("Error in main", e);
        }
    }
}