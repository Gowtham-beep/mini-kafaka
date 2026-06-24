package com.minibroker.testing;

import com.minibroker.Consumer;
import com.minibroker.Producer;
import com.minibroker.raft.rpc.RecordMetaData;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(EmbeddedBrokerExtension.class)
@EmbeddedBroker
public class EmbeddedBrokerExtensionTest {

    @InjectBroker
    private Producer producer;

    @InjectBroker
    private Consumer consumer;

    @Test
    public void testProduceAndConsume() throws Exception {
        assertNotNull(producer, "Producer should be injected");
        assertNotNull(consumer, "Consumer should be injected");

        byte[] payload = "Hello, Embedded Broker!".getBytes();

        CompletableFuture<RecordMetaData> produceFuture = producer.send(payload);
        RecordMetaData metaData = produceFuture.get(5, TimeUnit.SECONDS);

        assertNotNull(metaData);
        assertEquals(0, metaData.logicalOffset());

        CompletableFuture<java.util.List<byte[]>> consumeFuture = consumer.read(0, 1, 5000);
        java.util.List<byte[]> consumedPayloads = consumeFuture.get(5, TimeUnit.SECONDS);

        assertNotNull(consumedPayloads);
        assertEquals(1, consumedPayloads.size());
        assertArrayEquals(payload, consumedPayloads.get(0));
    }
}
