package com.anishek;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.io.StringWriter;

public class HashTest {

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
     * hash-max-ziplist-entries = 100002 , takes 1.12 MB of memory starting from 0.49 MB. Time taken 99 sec
     * hash-max-ziplist-entries = 512 , takes 6.42 MB of memory starting from 0.5 MB. Time Taken about 3 sec
     */
    @Test
    public void hundredThousandHash() {
        long hundredThousand = 100 * 1000;
        System.out.println(redis.info("memory"));
        long start = System.currentTimeMillis();
        for (long i = 0; i < hundredThousand; i++) {
            redis.hset("a", String.valueOf(i), "1");
        }
        long end = System.currentTimeMillis();
        System.out.println(redis.info("memory"));
        System.out.println("Time Taken(ms): " + (end - start));
    }

    /**
     * hash-max-ziplist-value = 64
     * -- uses 384 bytes when the value is of 100 bytes
     * -- uses 176 bytes when value is of 64 bytes
     */
    @Test
    public void singleHashLargeValue() {
        System.out.println(redis.info("memory"));
        redis.hset("a", "1", string(64));
        System.out.println(redis.info("memory"));
    }

    /**
     * hash value is of size 100 bytes
     * hash-max-ziplist-value = 128 -- size from 0.5MB to 109 MB --time approx 35sec
     * hash-max-ziplist-value = 64 --size from 0.5MB to 162 MB --time approx 30sec
     */
    @Test
    public void hashValueTest() {
        String value = string(100);
        long entries = 500;
        long oneMillion = 1000 * 1000;
        System.out.println(redis.info("memory"));
        long start = System.currentTimeMillis();
        for (long i = 0; i < oneMillion; i++) {
            redis.hset(String.valueOf(i / entries), String.valueOf(i % entries), value);
        }
        long end = System.currentTimeMillis();
        System.out.println(redis.info("memory"));
        System.out.println("Time Taken(ms): " + (end - start));
    }

    private String string(int numberOfBytes) {
        StringWriter stringWriter = new StringWriter();

        for (int i = 0; i < numberOfBytes; i++) {
            stringWriter.append("A");
        }
        return stringWriter.toString();
    }
}
