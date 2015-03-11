package com.anishek;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import redis.clients.jedis.BitOP;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * Two instances of redis running on localhost with ports 6379,7001 and twemproxy configured as below
 */
//alpha:
//    listen: 127.0.0.1:22121
//    hash: fnv1a_64
//    distribution: ketama
//    auto_eject_hosts: false
//    redis: true
//    server_retry_timeout: 2000
//    server_failure_limit: 1
//    servers:
//       - 127.0.0.1:6379:1 server1
//       - 127.0.0.1:7001:2 server2


public class TwemproxyTest {
    private String instanceOneKey = "anishek7173";
    private String instanceTwoKey = "anishek2240";

    private Jedis redis;

    @Before
    public void setup() {
        Jedis first = new Jedis("localhost");
        first.flushAll();
        first.close();
        Jedis second = new Jedis("localhost", 7001);
        second.flushAll();
        second.close();
        redis = new Jedis("localhost", 22121);
    }

    @After
    public void tearDown() {
        redis.close();
    }

    /**
     * The below keys go to different instances
     */
    @Test
    public void pipelinning() {
        Pipeline pipelined = redis.pipelined();
        pipelined.hset(instanceOneKey, "1", "1");
        pipelined.hset(instanceTwoKey, "1", "1");
        pipelined.hincrBy(instanceOneKey, "1", 1);
        pipelined.hincrBy(instanceTwoKey, "1", 2);
        pipelined.sync();

        assertThat(redis.hget(instanceOneKey, "1"), is("2"));
        assertThat(redis.hget(instanceTwoKey, "1"), is("3"));
    }


    @Test
    public void bitSetOperations() {
        Pipeline pipelined = redis.pipelined();
        pipelined.setbit(instanceTwoKey, 10, true);
        pipelined.setbit(instanceOneKey, 10, true);

        pipelined.setbit(instanceTwoKey, 100, true);
        pipelined.setbit(instanceOneKey, 100, true);

        pipelined.setbit(instanceTwoKey, 1, true);
        pipelined.setbit(instanceOneKey, 1, true);

        pipelined.setbit(instanceTwoKey, 113, true);
        pipelined.setbit(instanceOneKey, 113, true);

        pipelined.setbit(instanceTwoKey, 198, true);
        pipelined.setbit(instanceOneKey, 198, true);

        pipelined.sync();


        assertThat(redis.bitcount(instanceOneKey), is(5l));
        assertThat(redis.bitcount(instanceTwoKey), is(5l));

        /**
         * No bit operations supported on twemproxy
         */
        redis.bitop(BitOP.XOR, "result", instanceOneKey, instanceTwoKey);
        assertThat(redis.bitcount("result"), is(0l));
    }

    @Test
    @Ignore("Use this only to generate enough keys across instances")
    public void generateKeys() {
        for (int i = 0; i < 10000; i++) {
            redis.set("anishek" + i, String.valueOf(i));
        }
    }
}
