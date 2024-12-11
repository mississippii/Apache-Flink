package org.example;
import java.util.Properties;

public class Main {
    public static void main(String[] args) throws Exception {
        try {
            FlinkPipeline pipeline = new FlinkPipeline();
            pipeline.createPipeline();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}