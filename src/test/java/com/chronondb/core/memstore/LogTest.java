package com.chronondb.core.memstore;

import com.chronondb.core.exception.DatabaseGenericException;
import com.chronondb.core.properties.DefaultLogProperties;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class LogTest {

    @Test
    public void testBaseSimple() throws InterruptedException, DatabaseGenericException {
        // one thread, simple read-wite chain

        Log<Long, Integer> log = new Log<>(new DefaultLogProperties());

        // Add 123
        log.add(123L, Long.MAX_VALUE, null);

        // read range
        Assert.assertEquals((long) log.get(0, Long.MAX_VALUE).iterator().next().getId(), 123L);

        // read key
        Assert.assertEquals((long) log.get(123L).getId(), 123L);

        // Remove 123
        log.remove(123L);

        // read range
        Assert.assertTrue(log.get(0, Long.MAX_VALUE).isEmpty());

        // read key
        Assert.assertNull(log.get(123L));
    }

    @Test
    public void testVacuumSafetySimple() throws InterruptedException, DatabaseGenericException {
        // Vacuum must not remove any block
        Log<Long, Integer> log = new Log<>(new DefaultLogProperties());

        // Add 123
        log.add(123L, Long.MAX_VALUE, null);

        // read range
        Assert.assertEquals((long) log.get(0, Long.MAX_VALUE).iterator().next().getId(), 123L);

        // read key
        Assert.assertEquals((long) log.get(123L).getId(), 123L);

        int res = log.vacuum();
        Assert.assertEquals(res, 0);

        // read range
        Assert.assertEquals((long) log.get(0, Long.MAX_VALUE).iterator().next().getId(), 123L);

        // read key
        Assert.assertEquals((long) log.get(123L).getId(), 123L);
    }

    @Test
    public void testFlushSimple() throws InterruptedException, DatabaseGenericException {
        Log<Long, Integer> log = new Log<>(new DefaultLogProperties());

        // Add 123
        log.add(123L, Long.MAX_VALUE, null);

        // read range
        Assert.assertEquals((long) log.get(0, Long.MAX_VALUE).iterator().next().getId(), 123L);

        // read key
        Assert.assertEquals((long) log.get(123L).getId(), 123L);

        // flush
        log.flush(Long.MIN_VALUE, Long.MAX_VALUE);

        // read range
        Assert.assertTrue(log.get(0, Long.MAX_VALUE).isEmpty());

        // read key
        Assert.assertNull(log.get(123L));
    }

    @Test
    public void testTTLSimple() throws InterruptedException, DatabaseGenericException {
        Log<Long, Integer> log = new Log<>(new DefaultLogProperties());

        // Add 123
        log.add(123L, System.currentTimeMillis() + 100, null);

        // read key
        Assert.assertEquals((long) log.get(123L).getId(), 123L);

        // sleep
        Thread.sleep(101);

        // read key
        Assert.assertNull(log.get(123L));
    }

    @Test
    public void simpleStabilityMT1Test() throws InterruptedException, ExecutionException {
        LogMTRunner.runLogSimplePerformanceMTTest(1, 10*1000);
        System.gc();
    }

    @Test
    public void simpleStabilityMT10Test() throws InterruptedException, ExecutionException {
        LogMTRunner.runLogSimplePerformanceMTTest(10, 10*1000);
        System.gc();
    }

    @Test
    public void simpleStabilityMT100Test() throws InterruptedException, ExecutionException {
        LogMTRunner.runLogSimplePerformanceMTTest(100, 10*1000);
        System.gc();
    }

    @Test
    public void simpleStabilityMT200Test() throws InterruptedException, ExecutionException {
        LogMTRunner.runLogSimplePerformanceMTTest(200, 10*1000);
        System.gc();
    }



}
