package com.anishek;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class KeyValueTest {

    private Jedis redis;

    @Before
    public void setUp() {
        redis = new Jedis("localhost");
        redis.flushAll();
    }

    @After
    public void tearDown() {
        redis.close();
    }

    @Test
    @Ignore("Too Large, try out specifically")
    public void hundredMillionKeyValue() {
        long hundredMillion = 100 * 1000 * 1000;
        insertKeyValue(hundredMillion);
    }

    private void insertKeyValue(long numberOfKeys) {
        System.out.println(redis.info("memory"));
        long start = System.currentTimeMillis();
        for (long i = 0; i < numberOfKeys; i++) {
            redis.set("key" + i, String.valueOf(i));
        }
        long end = System.currentTimeMillis();
        System.out.println(redis.info("memory"));
        System.out.println("Time Taken(ms): " + (end - start));
    }

    private void pipelinedInsertKeyValue(long numberOfKeys, long pipelinedCommands) {
        System.out.println(redis.info("memory"));
        long start = System.currentTimeMillis();
        for (long i = 0; i < numberOfKeys; i += pipelinedCommands) {
            Pipeline pipelined = redis.pipelined();
            for (long j = 0; j < pipelinedCommands; j++) {
                redis.set("key" + i + j, String.valueOf(i + j));
            }
            pipelined.sync();
        }
        long end = System.currentTimeMillis();
        System.out.println(redis.info("memory"));
        System.out.println("Pipelined: " + pipelinedCommands + " Time Taken(ms): " + (end - start));
    }

    @Test
    public void oneMillionKeyValue() {
        long oneMillion = 1000 * 1000;
        insertKeyValue(oneMillion);
        redis.flushAll();
        pipelinedInsertKeyValue(oneMillion, 1000);
        redis.flushAll();
        pipelinedInsertKeyValue(oneMillion, 10000);
        /**
         * For local redis instance no time difference between the pipelined and regular execution.
         */
    }

    @Test
    public void oneMillionKeyValueWithMultipleThreads() throws InterruptedException {
        long oneMillion = 1000 * 1000;
        int numberOfThreads = 6;
        ExecutorService executorService = Executors.newFixedThreadPool(numberOfThreads, new ThreadFactoryBuilder().setNameFormat("%d").build());
        long perThread = oneMillion / numberOfThreads;

        System.out.println(redis.info("memory"));

        long start = System.currentTimeMillis();
        for (int i = 0; i < numberOfThreads; i++) {
            executorService.submit(new ThreadInsert(i * perThread, (i + 1) * perThread));
        }
        executorService.shutdown();
        executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);

        long end = System.currentTimeMillis();
        System.out.println(redis.info("memory"));
        System.out.println("Time Taken(ms): " + (end - start));
        /**
         * Looks like for 1 million keys performance doesnt increase after 5 threads and decreases at 4 and below with a 1 sec difference seen.
         */
    }

    public static class ThreadInsert implements Runnable {

        private final Jedis redis;
        private final long start;
        private final long end;

        public ThreadInsert(long start, long end) {
            this.redis = new Jedis("localhost");
            this.start = start;
            this.end = end;
        }

        @Override
        public void run() {
            for (long i = start; i < end; i++) {
                redis.set("key:" + i, String.valueOf(i));
            }
            redis.close();
        }
    }
}
