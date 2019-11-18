package com.bigapps.kafka.transaction;

import com.bigapps.kafka.transaction.producer.FormationProducer;
import com.test.spark.wiki.extracts.domains.HeartBeat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.ExecutionException;

public class HeartBeatApp {

    public static void main(String...args) throws IOException, ExecutionException, InterruptedException {
        BasicConfigurator.configure();

        FormationProducer<HeartBeat> heartBeatProducer = new FormationProducer<>();
        for (int i =0; i<100; i++){
            HeartBeat tx = HeartBeat.builder()
                    .appName("A")
                    .sentTime(new Date().getTime())
                    .build();
            heartBeatProducer.send(tx);
            Thread.sleep(5000);
        }
    }
}
