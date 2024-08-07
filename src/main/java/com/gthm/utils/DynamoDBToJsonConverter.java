package com.gthm.utils;

import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class DynamoDBToJsonConverter {

    public static String convertToJson(Map<String, AttributeValue> newImage) {
        // Assuming 'newImage' is your Map<String, AttributeValue>

        // Convert the Map<String, AttributeValue> to a Map<String, Object>
        Map<String, Object> attributeValueMap = convertToSimpleMap(newImage);

        // Convert the Map<String, Object> to a JSON string
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            String jsonString = objectMapper.writeValueAsString(attributeValueMap);
//            System.out.println(jsonString);
            return jsonString;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private static Map<String, Object> convertToSimpleMap(Map<String, AttributeValue> attributeValueMap) {
        Map<String, Object> simpleMap = new HashMap<>();
        if(attributeValueMap != null) {
            for (Map.Entry<String, AttributeValue> entry : attributeValueMap.entrySet()) {
                simpleMap.put(entry.getKey(), convertAttributeValue(entry.getValue()));
            }
        }
        return simpleMap;
    }

    private static Object convertAttributeValue(AttributeValue attributeValue) {
        if (attributeValue.getS() != null) {
            return attributeValue.getS();
        } else if (attributeValue.getN() != null) {
            return attributeValue.getN();
        } else if (attributeValue.getBOOL() != null) {
            return attributeValue.getBOOL();
        } else if (attributeValue.getM() != null) {
            return convertToSimpleMap(attributeValue.getM());
        } else if (attributeValue.getL() != null) {
            return attributeValue.getL().stream()
                    .map(DynamoDBToJsonConverter::convertAttributeValue)
                    .collect(Collectors.toList());
        } else if (attributeValue.getBS() != null) {
            return attributeValue.getBS();
        } else if (attributeValue.getSS() != null) {
            return attributeValue.getSS();
        } else if (attributeValue.getNS() != null) {
            return attributeValue.getNS();
        }
        return null;
    }


}
