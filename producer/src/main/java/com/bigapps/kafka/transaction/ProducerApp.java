package com.bigapps.kafka.transaction;

import com.bigapps.kafka.transaction.domain.Transaction;
import com.bigapps.kafka.transaction.producer.TransactionProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class ProducerApp {

    public static void main(String...args) throws IOException, ExecutionException, InterruptedException {
        BasicConfigurator.configure();
        Transaction tx = new Transaction("id_", new Double(5));
        TransactionProducer.sendTransaction(tx);
    }
}
