package com.anuchandy.servicebus;

import com.azure.messaging.servicebus.ServiceBusReceivedMessage;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

public final class AzureSBMsgProcessor implements Callable<List<RecordChange>> {
    private final int maxMessages = 100;
    private final Duration timeout = Duration.ofSeconds(7);
    private final SBReceiverCache.SBReceiver receiver;
    private final ExecutorService executorService;

    AzureSBMsgProcessor(int id, ExecutorService executorService) {
        this.receiver = SBReceiverCache.INSTANCE.getReceiver(id);
        this.executorService = executorService;
    }

    @Override
    public List<RecordChange> call() {
        final List<ServiceBusReceivedMessage> messages = receiver.receive(maxMessages, timeout, executorService);
        return processMessages(messages);
    }

    private List<RecordChange> processMessages(List<ServiceBusReceivedMessage> messages) {
        final List<RecordChange> recordChanges = new ArrayList<>(messages.size());
        for (ServiceBusReceivedMessage message : messages) {
            recordChanges.add(processMessage(message));
        }
        return recordChanges;
    }

    private RecordChange processMessage(ServiceBusReceivedMessage message) {
        // Map message to RecordChange.
        return new RecordChange();
    }
}
