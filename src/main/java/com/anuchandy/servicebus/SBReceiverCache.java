package com.anuchandy.servicebus;

import com.azure.core.util.ConfigurationBuilder;
import com.azure.core.util.logging.ClientLogger;
import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.ServiceBusReceiverClient;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public final class SBReceiverCache {
    public static final SBReceiverCache INSTANCE = new SBReceiverCache(Config.CONCURRENT_RECEIVERS, Config.TOPIC, Config.SUBSCRIPTION, Config.CONNECTION_STRING);
    private final int receiverCount;
    private final ConcurrentHashMap<Integer, SBReceiver> cache;

    private SBReceiverCache(int receiverCount, String topicName, String subscriptionName, String connectionString) {
        if (receiverCount <= 0) {
            throw new IllegalArgumentException("count must be greater than 0");
        }
        this.receiverCount = receiverCount;
        Objects.requireNonNull(topicName, "topicName must not be null");
        Objects.requireNonNull(subscriptionName, "subscriptionName must not be null");
        Objects.requireNonNull(connectionString, "connectionString must not be null");
        this.cache = new ConcurrentHashMap<>(receiverCount);
        for (int id = 0; id < receiverCount; id++) {
            cache.put(id, new SBReceiver(id, topicName, subscriptionName, connectionString));
        }
    }

    public SBReceiver getReceiver(int id) {
        if (id < 0 || id >= receiverCount) {
            throw new IllegalArgumentException("id must be between 0 and " + (receiverCount - 1));
        }
        return  cache.get(id);
    }

    public static final class SBReceiver {
        private static final String CLIENT_ID_KEY = "clientId";
        private final ClientLogger logger;
        private final Object lock = new Object();
        private final long receiveGracePeriodInSec = Duration.ofSeconds(7).toSeconds();
        private final String topicName;
        private final String subscriptionName;
        private final String connectionString;
        private final AtomicBoolean isReceiveActive = new AtomicBoolean(false);
        private final AtomicReference<Resource> resource = new AtomicReference<>(Resource.EMPTY);

        private SBReceiver(int id, String topicName, String subscriptionName, String connectionString) {
            final Map<String, Object> loggingContext = new HashMap<>(1);
            loggingContext.put(CLIENT_ID_KEY, id);
            this.logger = new ClientLogger(SBReceiver.class, loggingContext);
            this.topicName = topicName;
            this.subscriptionName = subscriptionName;
            this.connectionString = connectionString;
        }

        public List<ServiceBusReceivedMessage> receive(int maxMessages, Duration timeout, ExecutorService executorService) {
            if (maxMessages <= 0) {
                throw new IllegalArgumentException("maxMessages must be greater than 0");
            }
            Objects.requireNonNull(timeout, "timeout must not be null.");

            if (!isReceiveActive.compareAndSet(false, true)) {
                throw (new IllegalStateException("Application Error: Only one receive operation is allowed at a time."));
            }

            final ServiceBusReceiverClient innerReceiver;
            try {
                innerReceiver = getInnerReceiver();
            } catch (Exception e) {
                isReceiveActive.set(false);
                throw e;
            }

            final Future<List<ServiceBusReceivedMessage>> receiveWork;
            try {
                receiveWork = executorService.submit(() -> {
                    return innerReceiver.receiveMessages(maxMessages, timeout).stream().collect(Collectors.toList());
                });
            } catch (RejectedExecutionException e) {
                isReceiveActive.set(false);
                throw e;
            }

            try {
                final Duration timeoutWithGrace = timeout.plusSeconds(receiveGracePeriodInSec);
                final List<ServiceBusReceivedMessage> messages = receiveWork.get(timeoutWithGrace.toSeconds(), TimeUnit.SECONDS);
                return messages;
            } catch (Exception e) {
                dropInnerReceiver(e);
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                if (e instanceof TimeoutException) {
                    receiveWork.cancel(true);
                }
                return new ArrayList<>();
            } finally {
                isReceiveActive.set(false);
            }
        }

        public void completeMessage(ServiceBusReceivedMessage message) {
            final ServiceBusReceiverClient receiver = getInnerReceiver();
            receiver.complete(message);
        }

        private ServiceBusReceiverClient getInnerReceiver() {
            final Resource r0 = resource.get();
            if (r0 != Resource.EMPTY) {
                return r0.innerReceiver();
            }
            synchronized (lock) {
                final Resource r1 = resource.get();
                if (r1 != Resource.EMPTY) {
                    return r1.innerReceiver();
                }
                final ServiceBusReceiverClient receiver = buildInnerReceiver();
                resource.set(new Resource(receiver));
                return receiver;
            }
        }

        private void dropInnerReceiver(Exception e) {
            final Resource r0;
            synchronized (lock) {
                r0 = resource.getAndSet(Resource.EMPTY);
            }
            logger.atError().log("Dropping cached inner receiver due to error.", e);
            try {
                r0.close();
            } catch (Throwable t) {
                logger.atWarning().log("Error when closing cached inner receiver.", t);
            }
        }

        private ServiceBusReceiverClient buildInnerReceiver() {
            logger.atInfo().log("Building new cached inner receiver.");
            final ServiceBusReceiverClient innerReceiver = new ServiceBusClientBuilder()
                    .connectionString(connectionString)
                    // Enabling v2 stack in 7.15.0 for sync receive.
                    .configuration(new ConfigurationBuilder()
                            .putProperty("com.azure.messaging.servicebus.nonSession.syncReceive.v2", "true")
                            .build())
                    .receiver()
                    .topicName(topicName)
                    .subscriptionName(subscriptionName)
                    .buildClient();
            return innerReceiver;
        }

        private static class Resource {
            static final Resource EMPTY = new Resource(null);
            private final ServiceBusReceiverClient innerReceiver;

            private Resource(ServiceBusReceiverClient innerReceiver) {
                this.innerReceiver = innerReceiver;
            }

            ServiceBusReceiverClient innerReceiver() {
                return innerReceiver;
            }

            void close() {
                if (innerReceiver != null) {
                    innerReceiver.close();
                }
            }
        }
    }
}
