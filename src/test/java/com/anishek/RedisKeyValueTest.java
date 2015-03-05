package com.anishek;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

public class RedisKeyValueTest {

    private Jedis redis;

    @Before
    public void setUp() {
        redis = new Jedis("localhost");
        redis.flushAll();
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
    public void oneMillionKeyValueWithMultipleThreads() {
        
    }
}
