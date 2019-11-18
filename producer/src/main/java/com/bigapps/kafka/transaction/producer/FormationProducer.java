package com.bigapps.kafka.transaction.producer;

import com.test.spark.wiki.extracts.domains.FormationBean;
import com.test.spark.wiki.extracts.domains.FormationConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Properties;

public class FormationProducer<T extends FormationBean> {

    protected static Producer<Long, String> createProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", FormationConfig.BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    protected static final Producer<Long, String> producer = createProducer();

    protected static ObjectMapper objectMapper = new ObjectMapper();

    public void send(T tx) throws IOException {
        ProducerRecord<Long, String> record = new ProducerRecord<>(tx.getTopic(), objectMapper.writeValueAsString(tx));
        producer.send(record);
    }

}
