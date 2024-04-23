package org.example;
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