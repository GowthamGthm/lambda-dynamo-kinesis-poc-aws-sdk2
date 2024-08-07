package com.gthm.lambda.dynamodb;


import com.fasterxml.jackson.databind.ObjectMapper;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.gthm.lambda.dynamodb.model.STATUS;
import com.gthm.lambda.dynamodb.model.Student;
import com.gthm.utils.DynamoDBToJsonConverter;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class StudentLambda implements RequestHandler<DynamodbEvent, String> {
    DynamoDbClient ddb = DynamoDbClient.builder()
            .region(Region.US_WEST_1) // Specify your region
            .credentialsProvider(DefaultCredentialsProvider.create())
            .build();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final String tableName = "processedStudent";

    @Override
    public String handleRequest(DynamodbEvent dynamodbEvent, Context context) {


        for(DynamodbEvent.DynamodbStreamRecord record : dynamodbEvent.getRecords()) {

            String newImage = DynamoDBToJsonConverter.convertToJson(record.getDynamodb().getNewImage());
            String oldImage =  DynamoDBToJsonConverter.convertToJson(record.getDynamodb().getOldImage());

            System.out.println("dynamoDb NewImage: " + newImage);
            System.out.println("dynamoDb oldImage: " + oldImage);

            Student student = null;

            try {
                student = objectMapper.readValue(newImage, Student.class);
            } catch (IOException e) {
                e.printStackTrace();
            }


            Map<String, AttributeValue> itemValues = new HashMap<>();
            itemValues.put("id", AttributeValue.builder().s(
                    String.join(",", student.getId(),student.getName())).build());
            itemValues.put("name", AttributeValue.builder().s(student.getName()).build());
            itemValues.put("age", AttributeValue.builder().n(String.valueOf(student.getAge())).build());
            itemValues.put("status", AttributeValue.builder().s(STATUS.PROCESSED.toString()).build());

            // Create a PutItemRequest
            PutItemRequest request = PutItemRequest.builder()
                    .tableName(tableName)
                    .item(itemValues)
                    .build();

            ddb.putItem(request);

            System.out.println("udpated the PROCESSED student with values : " + request);


        }

        return "SUCCESS";
    }
}
