package com.gthm.lambda.kinesis;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;

import java.nio.charset.StandardCharsets;

public class OrdersLambda implements RequestHandler<KinesisEvent , String> {

//    orderStreamLambda
    @Override
    public String handleRequest(KinesisEvent event, Context context) {

        System.out.println("kinesis event: " +event);
        for(KinesisEvent.KinesisEventRecord record : event.getRecords()) {
            String data = StandardCharsets.UTF_8.decode(record.getKinesis().getData()).toString();
            System.out.println("data from record: " + data);
        }
        return "SUCCESS";
    }



}