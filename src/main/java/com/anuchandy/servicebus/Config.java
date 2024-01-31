package com.anuchandy.servicebus;

final class Config {
    public static int CONCURRENT_RECEIVERS = 5;
    public static int THREAD_POOL_SIZE = 10 * Runtime.getRuntime().availableProcessors();

    public static String TOPIC = System.getenv("SERVICEBUS_TOPIC");
    public static String SUBSCRIPTION = System.getenv("SERVICEBUS_SUBSCRIPTION");
    public static String CONNECTION_STRING = System.getenv("SERVICEBUS_CONNECTION_STRING");
}
