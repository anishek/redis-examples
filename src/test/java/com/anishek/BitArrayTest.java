package com.anishek;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class BitArrayTest {

    private Jedis redis;

    @Before
    public void setUp() {
        this.redis = new Jedis("localhost");
        this.redis.flushAll();
    }

    @After
    public void teardown() {
        this.redis.close();
    }

    /**
     * Same time to set key value or key bits
     */
    @Test
    public void oneMillionBits() {
        long oneMillion = 1000 * 1000;
        System.out.println(redis.info("memory"));
        long start = System.currentTimeMillis();
        for (long i = 0; i < oneMillion; i++) {
            redis.setbit("a", i, "1");
        }
        long end = System.currentTimeMillis();
        System.out.println(redis.info("memory"));
        System.out.println("Time Taken(ms): " + (end - start));
    }

    /**
     * This will go out of memory if the bit position increases. Using the same position results in almost similar usage
     * like key value test
     */
    @Test
    public void oneMillionBitsDifferentKeys() {
        long oneMillion = 1000 * 1000;
        System.out.println(redis.info("memory"));
        long start = System.currentTimeMillis();
        for (long i = 0; i < oneMillion; i++) {
            redis.setbit(String.valueOf(i), 0, true);
        }
        long end = System.currentTimeMillis();
        System.out.println(redis.info("memory"));
        System.out.println("Time Taken(ms): " + (end - start));
    }

    /**
     * Same performance across threads to set key value or key bits
     */
    @Test
    public void oneMillionBitsInThreads() throws InterruptedException {
        long oneMillion = 1000 * 1000;
        int numberOfThreads = 5;
        ExecutorService executorService = Executors.newFixedThreadPool(numberOfThreads, new ThreadFactoryBuilder().setNameFormat("%d").build());
        long perThread = oneMillion / numberOfThreads;

        System.out.println(redis.info("memory"));

        long start = System.currentTimeMillis();
        for (int i = 0; i < numberOfThreads; i++) {
            executorService.submit(new ThreadInsert("A", i * perThread, (i + 1) * perThread));
        }
        executorService.shutdown();
        executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);

        long end = System.currentTimeMillis();
        System.out.println(redis.info("memory"));
        System.out.println("Time Taken(ms): " + (end - start));
    }


    public static class ThreadInsert implements Runnable {

        private final Jedis redis;
        private final long start;
        private final long end;
        private final String key;

        public ThreadInsert(String key, long start, long end) {
            this.key = key;
            this.redis = new Jedis("localhost");
            this.start = start;
            this.end = end;
        }

        @Override
        public void run() {
            for (long i = start; i < end; i++) {
                redis.setbit(key, i, true);
            }
            redis.close();
        }
    }
}
