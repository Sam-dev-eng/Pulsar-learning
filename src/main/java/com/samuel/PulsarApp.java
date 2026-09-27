package com.samuel;

import org.apache.pulsar.client.api.*;

import java.util.concurrent.TimeUnit;

public class PulsarApp {
    public static final String PULSER_URL = "pulsar://localhost:6650";
    public static final String Topic = "persistent://public/default/order-events";
    private static final String SUBSCRIPTION = "order-sub";

    public static void main(String[] args) throws Exception {
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(PULSER_URL)
                .build();
        Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topic(Topic)
                .subscriptionName(SUBSCRIPTION)
                .subscriptionType(SubscriptionType.Shared)
                .subscribe();
        Producer<String> producer = client.newProducer(Schema.STRING)
                .topic(Topic)
                .create();

        for (int count = 1; count <= 5; count++) {
            Message<String> message = consumer.receive(5, TimeUnit.SECONDS);
            if (message != null) {
                System.out.println("[consumer] Received: " + message.getValue());
                consumer.acknowledge(message);
                System.out.println("[consumer] ACK " + message.getValue());
            }
        }

        producer.close();
        consumer.close();
        client.close();
        System.out.println("[App] Application pulsar stopped oooo");


    }
}
