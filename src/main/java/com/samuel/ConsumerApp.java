package com.samuel;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import java.util.concurrent.TimeUnit;


public class ConsumerApp {

    private static final String PULSAR_URL = "pulsar://localhost:6650";

    private static final String TOPIC = "persistent://public/default/order-events";

    private static final String SUBSCRIPTION = "order-sub";


    public static void main(String[] args) throws Exception {
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(PULSAR_URL)
                .build();

        Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topic(TOPIC)
                .subscriptionName(SUBSCRIPTION)
                .subscriptionType(SubscriptionType.Exclusive)
                .subscribe();

        System.out.println("[CONSUMER] Waiting for messages...");

        for (int count = 1; count <= 5; count++) {

            Message<String> message = consumer.receive(10, TimeUnit.SECONDS);

            if (message == null) {
                System.out.println("[CONSUMER] No message received.");
                continue;
            }

            System.out.println("[CONSUMER] Received: " + message.getValue());

            consumer.acknowledge(message);

            System.out.println("[CONSUMER] ACK: " + message.getValue());
        }

        consumer.close();
        client.close();

        System.out.println("[CONSUMER] Consumer stopped.");
    }
}
