package com.bigapps.kafka.transaction;

import com.bigapps.kafka.transaction.producer.FormationProducer;
import com.test.spark.wiki.extracts.domains.FormationConfig;
import com.test.spark.wiki.extracts.domains.HeartBeat;
import com.test.spark.wiki.extracts.domains.Transaction;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.ExecutionException;

public class TransactionApp {

    public static void main(String...args) throws IOException, ExecutionException, InterruptedException {
        BasicConfigurator.configure();

        FormationProducer<Transaction> producer = new FormationProducer<>();
        for (int i =0; i<100; i++){
            int accountId = i%3;
            Transaction tx = Transaction.builder()
                    .id("tx_id_"+i)
                    .account("ACCOUNT_"+accountId)
                    .date(new Date().getTime())
                    .type("CHEQUE")
                    .amount(Math.random()*1000)
                    .build();
            producer.send(tx);
        }
    }
}
