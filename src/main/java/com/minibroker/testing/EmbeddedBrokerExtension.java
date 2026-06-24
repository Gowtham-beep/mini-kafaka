package com.minibroker.testing;

import com.minibroker.Broker;
import com.minibroker.BrokerConfig;
import com.minibroker.Consumer;
import com.minibroker.MiniKafkaBroker;
import com.minibroker.Producer;
import com.minibroker.raft.RaftNode;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class EmbeddedBrokerExtension implements BeforeEachCallback, AfterEachCallback {

    private static final AtomicInteger portAllocator = new AtomicInteger(19000);

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        Class<?> testClass = context.getRequiredTestClass();
        EmbeddedBroker embeddedBroker = testClass.getAnnotation(EmbeddedBroker.class);
        if (embeddedBroker == null) {
            return;
        }

        int replicationFactor = embeddedBroker.replicationFactor();
        Map<String, InetSocketAddress> clusterAddresses = new HashMap<>();
        for (int i = 0; i < replicationFactor; i++) {
            clusterAddresses.put("node-" + i, new InetSocketAddress("127.0.0.1", portAllocator.getAndIncrement()));
        }

        List<MiniKafkaBroker> brokers = new ArrayList<>();
        List<Path> tempDirs = new ArrayList<>();

        ExtensionContext.Store store = context.getStore(ExtensionContext.Namespace.create(getClass(), context.getRequiredTestInstance()));

        for (int i = 0; i < replicationFactor; i++) {
            Path tempDir = Files.createTempDirectory("embedded-broker-");
            tempDirs.add(tempDir);
            String nodeId = "node-" + i;
            int port = clusterAddresses.get(nodeId).getPort();
            BrokerConfig config = new BrokerConfig(nodeId, tempDir, port, clusterAddresses);
            MiniKafkaBroker broker = new MiniKafkaBroker(config);
            broker.start();
            brokers.add(broker);
        }

        store.put("brokers", brokers);
        store.put("tempDirs", tempDirs);

        MiniKafkaBroker leaderBroker = null;
        long startTime = System.currentTimeMillis();
        long timeout = 3000;

        while (System.currentTimeMillis() - startTime < timeout) {
            int leaderCount = 0;
            MiniKafkaBroker potentialLeader = null;
            for (MiniKafkaBroker broker : brokers) {
                if (broker.getRole() == com.minibroker.Role.LEADER) {
                    leaderCount++;
                    potentialLeader = broker;
                }
            }
            if (leaderCount == 1) {
                leaderBroker = potentialLeader;
                break;
            }
            Thread.sleep(50);
        }

        if (leaderBroker == null) {
            throw new IllegalStateException("Failed to elect exactly one leader within 3 seconds");
        }

        RaftNode raftNode = leaderBroker.getRaftNode();
        ExecutorService consumerExecutor = Executors.newCachedThreadPool();
        store.put("consumerExecutor", consumerExecutor);

        Producer producer = new Producer(raftNode);
        Consumer consumer = new Consumer(raftNode, consumerExecutor);

        Object testInstance = context.getRequiredTestInstance();
        for (Field field : testClass.getDeclaredFields()) {
            if (field.isAnnotationPresent(InjectBroker.class)) {
                field.setAccessible(true);
                if (field.getType().isAssignableFrom(Producer.class)) {
                    field.set(testInstance, producer);
                } else if (field.getType().isAssignableFrom(Consumer.class)) {
                    field.set(testInstance, consumer);
                } else {
                    throw new IllegalArgumentException("@InjectBroker can only be applied to Producer or Consumer fields");
                }
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void afterEach(ExtensionContext context) throws Exception {
        ExtensionContext.Store store = context.getStore(ExtensionContext.Namespace.create(getClass(), context.getRequiredTestInstance()));

        List<MiniKafkaBroker> brokers = (List<MiniKafkaBroker>) store.get("brokers");
        if (brokers != null) {
            for (MiniKafkaBroker broker : brokers) {
                broker.shutdown();
            }
        }

        ExecutorService consumerExecutor = (ExecutorService) store.get("consumerExecutor");
        if (consumerExecutor != null) {
            consumerExecutor.shutdownNow();
        }

        List<Path> tempDirs = (List<Path>) store.get("tempDirs");
        if (tempDirs != null) {
            for (Path tempDir : tempDirs) {
                if (Files.exists(tempDir)) {
                    try (Stream<Path> walk = Files.walk(tempDir)) {
                        walk.sorted(Comparator.reverseOrder())
                            .map(Path::toFile)
                            .forEach(File::delete);
                    } catch (IOException e) {
                        // Ignore
                    }
                }
            }
        }
    }
}
