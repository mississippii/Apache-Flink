package org.example;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordProcessingMetric extends RichMapFunction<String, String> {

    private transient Counter wordCounter;
    private static final Logger LOG = LoggerFactory.getLogger(WordProcessingMetric.class);

    @Override
    public void open(Configuration parameters) {
        // Initialize the counter
        wordCounter = getRuntimeContext()
                .getMetricGroup()
                .counter("processedWords");
    }

    @Override
    public String map(String value) throws Exception {
        wordCounter.inc(); // Increment counter for each processed word
        LOG.info("Processed words: {}", wordCounter.getCount());
        return value;
    }
}
