package com.anuchandy.servicebus;

import com.azure.core.util.logging.ClientLogger;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

public final class App {
    private final ClientLogger logger;

    App() {
        this.logger = new ClientLogger(App.class);
    }

    public static void main(String[] args) {
        final App app = new App();
        final ExecutorService executorService = Executors.newFixedThreadPool(Config.THREAD_POOL_SIZE, new DefaultThreadFactory("receiver-thread"));
        final int batchSize = 5000;
        final int concurrency = Config.CONCURRENT_RECEIVERS;

        final List<AzureSBMsgProcessor> jobs = new ArrayList<>(concurrency);
        for (int id = 0; id < concurrency; id++) {
            jobs.add(new AzureSBMsgProcessor(id, executorService));
        }

        try {
            app.businessLogic(executorService, jobs, batchSize);
        } finally {
            shutdown(executorService, app.logger);
        }
    }

    private void businessLogic(ExecutorService executorService, List<AzureSBMsgProcessor> jobs, int batchSize) {
        final List<RecordChange> recordChangeList = new ArrayList<>();
        boolean isMsgPresentForDp = true;
        while(recordChangeList.size() < batchSize && isMsgPresentForDp) {
            final List<FutureTask<List<RecordChange>>> futureTaskList  = new ArrayList<>();
            for (AzureSBMsgProcessor job: jobs) {
                futureTaskList.add((FutureTask<List<RecordChange>>) executorService.submit(job));
            }

            List<RecordChange> tempRecordChangeList = new ArrayList<>();
            futureTaskList.forEach(msgList -> {
                try {
                    tempRecordChangeList.addAll(msgList.get());
                } catch (InterruptedException | ExecutionException ex) {
                    logger.atError().log("Error occurred while fetching values from future tasks", ex);
                    if (ex instanceof InterruptedException)
                        Thread.currentThread().interrupt();
                }
            });

            if (tempRecordChangeList.isEmpty()) {
                isMsgPresentForDp = false;
            } else {
                recordChangeList.addAll(tempRecordChangeList);
            }
        }
    }

    private static void shutdown(ExecutorService executorService, ClientLogger logger) {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException ex) {
            logger.atError().log("Error occurred while shutting down executor service", ex);
            Thread.currentThread().interrupt();
        }
    }
}
