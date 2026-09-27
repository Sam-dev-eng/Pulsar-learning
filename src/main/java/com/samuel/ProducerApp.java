package com.samuel;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;

public class ProducerApp {

    private static final String PULSAR_URL = "pulsar://localhost:6650";

    private static final String TOPIC = "persistent://public/default/order-events";

    public static void main(String[] args) throws Exception {

        PulsarClient client = PulsarClient.builder()
                .serviceUrl(PULSAR_URL)
                .build();

        Producer<String> producer = client.newProducer(Schema.STRING)
                .topic(TOPIC)
                .create();

        for (int count = 1; count <= 15; count++) {

            String message = "Order #" + count;

            System.out.println("[PRODUCER] Sending: " + message);

            producer.send(message);
            Thread.sleep(300);
        }

        System.out.println("[PRODUCER] Done.");

        producer.close();
        client.close();
    }
}
