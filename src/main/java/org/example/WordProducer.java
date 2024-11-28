package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class WordProducer {
    private static WordProducer producerObj;
    private  WordProducer() {
    }
    public static WordProducer getProducerObj(){
        if(producerObj == null){
             producerObj= new WordProducer();
        }
        return producerObj;
    }
    public  void startWordProduce(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "all");
        props.put("retries", 3);

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        try {
            int id=1;
            while (true) {
                String word = generateWord(id);
                System.out.println("Generate by Producer "+word);
                producer.send(new ProducerRecord<>("flink1", word));
                Thread.sleep(1000);
                id++;
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
    public static String generateWord(int id) {
        String[] words = {"hello", "world", "apache", "flink", "kafka", "stream", "example"};
        return words[(int) (Math.random() * words.length)]+" "+id;
    }
}
