package com.gthm.kinesis.producer.config;


import software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;

public class AwsKinesisClient {

    public static final String AWS_A_KEY_ID = "";
    public static final String AWS_S_KEY_ID = "";

    static {
        //add your AWS account access key and secret key
        System.setProperty(null, "");
        System.setProperty(null, "");
    }

    public static KinesisClient getKinesisClient() {

        KinesisClient kinesisClient = KinesisClient.builder()
                .region(Region.US_WEST_1)
                .credentialsProvider(SystemPropertyCredentialsProvider.create())
                .build();
        return kinesisClient;
    }
}