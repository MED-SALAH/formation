package com.bigapps.kafka.transaction;

import com.test.spark.wiki.extracts.domains.Transaction;
import com.bigapps.kafka.transaction.producer.TransactionProducer;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.ExecutionException;

public class ProducerApp {

    public static void main(String...args) throws IOException, ExecutionException, InterruptedException {
        BasicConfigurator.configure();
        for (int i =0; i<100; i++){
            int accountId = i%3;
            Transaction tx = Transaction.builder()
                    .id("tx_id_"+i)
                    .account("ACCOUNT_"+accountId)
                    .date(new Date().getTime())
                    .type("CHEQUE")
                    .amount(Math.random()*1000)
                    .build();
            TransactionProducer.sendTransaction(tx);
        }
    }
}
