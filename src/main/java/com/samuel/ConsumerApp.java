package com.samuel;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import java.util.concurrent.TimeUnit;


public class ConsumerApp {

    private static final String PULSAR_URL = "pulsar://localhost:6650";

    private static final String TOPIC = "persistent://public/default/order-events-partitioned";

    private static final String SUBSCRIPTION = "partitioned-shared-sub";


    public static void main(String[] args) throws Exception {
        String consumerName = args.length > 0 ? args[0] : "consumer-1";

        PulsarClient client = PulsarClient.builder()
                .serviceUrl(PULSAR_URL)
                .build();
        Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topic(TOPIC)
                .subscriptionName(SUBSCRIPTION)
                .subscriptionType(SubscriptionType.Shared)
                .consumerName(consumerName)
                .subscribe();

        System.out.printf("[%s] waiting for messages...%n",consumerName);

        while(true){
            Message<String> message = consumer.receive(30,TimeUnit.SECONDS);

            if (message == null){
                System.out.printf("[%s] No message received%n",consumerName);
                continue;
            }

            System.out.println("[" + consumerName + "] Received: " + message.getValue() + " | Topic: " + message.getTopicName());

            consumer.acknowledge(message);
            System.out.printf("[%s] Ack: %s%n",consumerName,message.getValue());
        }

    }
}
