package com.gthm.kinesis.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.gthm.kinesis.producer.config.AwsKinesisClient;
import com.gthm.kinesis.producer.model.Order;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * Hello world
 */
public class KinesisProducer {
    private static final String ORDER_STREAM_NAME = "OrderStream";
    List<String> productList = new ArrayList<>();

    Random random = new Random();

    public static void main(String[] args) throws InterruptedException, JsonProcessingException {
        KinesisProducer app = new KinesisProducer();
        app.populateProductList();
        //1. get client
        KinesisClient kinesisClient = AwsKinesisClient.getKinesisClient();

        while (true) {
            app.sendData(kinesisClient);
            Thread.sleep(5000);
        }

    }

    private void sendData(KinesisClient kinesisClient) throws JsonProcessingException {
        //2. PutRecordRequest
        PutRecordsRequest recordsRequest = PutRecordsRequest.builder()
                .streamName(ORDER_STREAM_NAME)
                .records(getRecordsRequestList()).build();

        //3. putRecord or putRecords - 500 records with single API call
        PutRecordsResponse results = kinesisClient.putRecords(recordsRequest);
        if (results.failedRecordCount() > 0) {
            System.out.println("Error occurred for records " + results.failedRecordCount());
        } else {
            System.out.println("Data sent successfully...");
        }

    }

    private List<PutRecordsRequestEntry> getRecordsRequestList() throws JsonProcessingException {
//        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.enable(SerializationFeature.INDENT_OUTPUT);

        List<PutRecordsRequestEntry> putRecordsRequestEntries = new ArrayList<>();
        for (Order order : getOrderList()) {
            PutRecordsRequestEntry.Builder requestEntry = PutRecordsRequestEntry.builder();

            requestEntry.data(SdkBytes.fromString(objectMapper.writeValueAsString(order), Charset.defaultCharset()));
            requestEntry.partitionKey(UUID.randomUUID().toString());
            putRecordsRequestEntries.add(requestEntry.build());

        }
        return putRecordsRequestEntries;
    }

    private List<Order> getOrderList() {
        List<Order> orders = new ArrayList<>();
        for (int i = 1; i <= 500; i++) {
            Order order = new Order();
            order.setOrderId(Math.abs(random.nextInt()));
            order.setProduct(productList.get(random.nextInt(productList.size())));
            order.setQuantity(random.nextInt(20));
            orders.add(order);
        }
        return orders;
    }

    private void populateProductList() {
        productList.add("shirt");
        productList.add("t-shirt");
        productList.add("shorts");
        productList.add("tie");
        productList.add("shoes");
        productList.add("jeans");
        productList.add("belt");
    }

}
