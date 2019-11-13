package com.bigapps.kafka.transaction.producer;
import com.test.spark.wiki.extracts.domains.Transaction;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class TransactionProducer {

    private final static String TOPIC = "Transaction";
    private final static String BOOTSTRAP_SERVERS = "15.188.51.222:9092";

    private static ObjectMapper objectMapper = new ObjectMapper();

    static final Producer<Long, String> p = createProducer();

    private static Producer<Long, String> createProducer() {
        Properties props = new Properties();

        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    public static void sendTransaction(Transaction tx) throws IOException, ExecutionException, InterruptedException {
        ProducerRecord<Long, String> record = new ProducerRecord<>(TOPIC, objectMapper.writeValueAsString(tx));
        p.send(record).get();
    }
}
