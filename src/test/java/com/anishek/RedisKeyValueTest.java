package com.anishek;

import org.junit.Test;
import redis.clients.jedis.Jedis;

public class RedisKeyValueTest {

    @Test
    public void HundredMillionKeyValue() {
        Jedis redis = new Jedis("localhost");
        long hundredMillion = 100 * 1000 * 1000;
        for (long i = 0; i < hundredMillion; i++) {
            redis.set("key" + i, String.valueOf(i));
        }

        System.out.println(redis.info("memory"));
    }
}
