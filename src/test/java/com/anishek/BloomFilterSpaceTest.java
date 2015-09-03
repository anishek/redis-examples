package com.anishek;

import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.FilterBuilder;
import org.junit.Test;
import redis.clients.jedis.Jedis;

/**
 * using the calculation on wiki for bloom
 * for 100 million cardinality with 0.001 false positive per key bit space = 100000000 * 1.44 ln 1000 = looks like 120 MB
 */
public class BloomFilterSpaceTest {

    @Test
    public void populate() {

        int cardinality = Integer.parseInt(System.getProperty("cardinality"));
        String host = System.getProperty("host");
        int port = Integer.parseInt(System.getProperty("port"));
        double falsePositiveRate = Double.parseDouble(System.getProperty("falsePositive"));

        Jedis jedis = new Jedis(host, port);

        System.out.println("Before:" + jedis.info("memory").split("\n")[2]);

        BloomFilter<Long> bf = new FilterBuilder()
                .redisHost(host)
                .redisPort(port)
                .redisBacked(true)
                .expectedElements(cardinality)
                .falsePositiveProbability(falsePositiveRate)
                .name("test")
                .buildBloomFilter();
        bf.clear();

        System.out.println("After Clear:" + jedis.info("memory").split("\n")[2]);

        int falsePositives = 0;
        int elements = (int) ((1 - falsePositiveRate) * cardinality);
        for (long i = 0; i < elements; i++) {
            if (bf.contains(i)) {
                falsePositives++;
            } else {
                bf.add(i);
            }
        }

        System.out.println("After Inserting " + elements + " elements :" + jedis.info("memory").split("\n")[2]);
        System.out.println("false positive : " + falsePositives + " cardinality : " + cardinality + " elements: " + elements);

        jedis.close();
    }
}
