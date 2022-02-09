package com.chronondb.core.memstore;

import com.chronondb.core.exception.DatabaseGenericException;
import com.chronondb.core.memstore.Log;
import com.chronondb.core.memstore.LogItem;
import com.chronondb.core.properties.DefaultLogProperties;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.LinkedList;

public class LogTest_Performance_Single {

    @Test
    public void linearReadEraseBlockTest() throws InterruptedException, DatabaseGenericException {
        // one-thread block read-erase-vacuum

        Log<Long, Integer> log = new Log<Long, Integer>(new DefaultLogProperties());

        int BLOCK_SIZE = 10 * 1000 * 1000;

        long opStart = System.currentTimeMillis();
        long opTime = System.currentTimeMillis() - opStart;

        // READ BLOCK
        var data = readRange(log, Long.MIN_VALUE, Long.MAX_VALUE, 0, "Full read on empty log");
        Assert.assertEquals(data.size(), 0);

        // WRITE
        writeBlock(log, BLOCK_SIZE, 0, Long.MAX_VALUE, "Simple write partition 0 to clean log");

        // READ BLOCK
        data = readRange(log, Long.MIN_VALUE, Long.MAX_VALUE, BLOCK_SIZE, "Full read after write");
        Assert.assertEquals(data.size(), BLOCK_SIZE);

        // ERASE BLOCK
        eraseBlock(log, BLOCK_SIZE, 0, "Erase partition 0");

        // READ BLOCK
        data = readRange(log, Long.MIN_VALUE, Long.MAX_VALUE, BLOCK_SIZE, "Full read after full erase");
        Assert.assertEquals(data.size(), 0);

        // VACUUM
        vacuum(log, "VACUUM everything");

        // READ BLOCK
        data = readRange(log, Long.MIN_VALUE, Long.MAX_VALUE, BLOCK_SIZE, "Full read after vacuum");
        Assert.assertEquals(data.size(), 0);

    }

    @Test
    public void linearRotationTest() throws InterruptedException, DatabaseGenericException {
        // one-thread internal block rotation consistency test

        Log<Long, Integer> log = new Log<Long, Integer>(new DefaultLogProperties().setBlockSize(1));

        int BLOCK_SIZE=1000000;

        writeBlock(log, BLOCK_SIZE, 0, Long.MAX_VALUE, "Write 1 msec blocks");

        var data = readRange(log, Long.MIN_VALUE, Long.MAX_VALUE, BLOCK_SIZE, "Full read again");
        Assert.assertEquals(data.size(), BLOCK_SIZE);

        eraseBlock(log, BLOCK_SIZE, 0, "Erasing everything");

        data = readRange(log, Long.MIN_VALUE, Long.MAX_VALUE, BLOCK_SIZE, "Full read after erase");
        Assert.assertEquals(data.size(), 0);

        log.vacuum();

        data = readRange(log, Long.MIN_VALUE, Long.MAX_VALUE, BLOCK_SIZE, "Full read after vacuum");
        Assert.assertEquals(data.size(), 0);

    }


    @Test
    public void linearLongChainSwapTest() throws InterruptedException, DatabaseGenericException {
        // one-thread internal block rotation consistency test with 1 record per block situation

        Log<Long, Integer> log = new Log<Long, Integer>(new DefaultLogProperties().setBlockSize(1));

        int CHAIN_SIZE = 10000;

        for (int i = 0; i < CHAIN_SIZE; i++) {
            log.add((long) i, Long.MAX_VALUE, null);
            Thread.sleep(1);
        }

        var data = readRange(log, Long.MIN_VALUE, Long.MAX_VALUE, CHAIN_SIZE, "Full read again");
        Assert.assertEquals(data.size(), CHAIN_SIZE);

        eraseBlock(log, CHAIN_SIZE, 0, "Erasing everything");

        data = readRange(log, Long.MIN_VALUE, Long.MAX_VALUE, CHAIN_SIZE, "Full read after erase");
        Assert.assertEquals(data.size(), 0);

        log.vacuum();

        data = readRange(log, Long.MIN_VALUE, Long.MAX_VALUE, CHAIN_SIZE, "Full read after vacuum");
        Assert.assertEquals(data.size(), 0);

    }


    @Test
    public void linearSimpleBlockTTLTest() throws InterruptedException, DatabaseGenericException {
        // one-threaded TTL test

        Log<Long, Integer> log = new Log<Long, Integer>(new DefaultLogProperties());

        int BLOCK_SIZE = 10 * 1000 * 1000;

        long opStart = System.currentTimeMillis();
        long opTime = System.currentTimeMillis() - opStart;

        // WRITE
        writeBlock(log, BLOCK_SIZE, 0, Long.MAX_VALUE, "Write partition 0 no TTL");

        // WRITE
        writeBlock(log, BLOCK_SIZE, 1, System.currentTimeMillis() + 5000, "Write partition 1 with TTL + 5000");

        // READ BLOCK
        var data = readRange(log, Long.MIN_VALUE, Long.MAX_VALUE, 2*BLOCK_SIZE, "Full read again");
        Assert.assertEquals(data.size(), 2*BLOCK_SIZE);

        // WAIT TTL
        System.out.println("Waiting 6 sec for TTL to expire");
        Thread.sleep(6000);
        System.out.println("Wait complete");

        // READ BLOCK
        data = readRange(log, Long.MIN_VALUE, Long.MAX_VALUE, 2*BLOCK_SIZE, "Full read again");
        Assert.assertEquals(data.size(), BLOCK_SIZE);

        // VACUUM
        vacuum(log, "VACUUM everything");

        // READ BLOCK
        data = readRange(log, Long.MIN_VALUE, Long.MAX_VALUE, BLOCK_SIZE, "Full read again");
        Assert.assertEquals(data.size(), BLOCK_SIZE);

        // ERASE BLOCK
        eraseBlock(log, BLOCK_SIZE, 0, "Erase partition 0");

        // READ BLOCK
        data = readRange(log, Long.MIN_VALUE, Long.MAX_VALUE, 0, "Full read again");
        Assert.assertEquals(data.size(), 0);

        // VACUUM
        vacuum(log, "VACUUM everything");

        // READ BLOCK
        data = readRange(log, Long.MIN_VALUE, Long.MAX_VALUE, 0, "Full read again");
        Assert.assertEquals(data.size(), 0);

    }

    private void vacuum(Log<Long, Integer> log, String note) {
        long opStart = System.currentTimeMillis();
        log.vacuum();
        long opTime = System.currentTimeMillis() - opStart;
        System.out.println(" --- VACUUM ---");
        System.out.println(" Note: " + note);
        System.out.println(" Total time, ms = " + opTime);

    }

    private void eraseBlock(Log<Long, Integer> log, int blockSize, int partition, String note) throws DatabaseGenericException {
        long opStart = System.currentTimeMillis();
        for (long i = (long) partition * blockSize; i < (long) (partition + 1) * blockSize; i++) {
            log.remove(i);
        }
        long opTime = System.currentTimeMillis() - opStart;
        System.out.println(" --- ERASE " + blockSize + " records ---");
        System.out.println(" Note: " + note);
        System.out.println(" Total time, ms = " + opTime);
        if (opTime ==0) opTime = 1;
        System.out.println(" TPMS = " + blockSize / opTime);
        System.out.println(" TPS = " + (blockSize * 1000L) / opTime);
    }

    private Collection<LogItem<Long, Integer>> readRange(Log<Long, Integer> log, long start, long end, int dirtyRecords, String note) throws DatabaseGenericException {
        long opStart = System.currentTimeMillis();
        var data = log.get(start, end);
        long opTime = System.currentTimeMillis() - opStart;
        System.out.println(" --- READING data ---");
        System.out.println(" Note: " + note);
        System.out.println(" Total time, ms = " + opTime);
        System.out.println(" Data read, records = " + data.size());
        if (opTime ==0) opTime = 1;
        System.out.println(" Records scanned / ms " + dirtyRecords / opTime);
        System.out.println(" Records found / ms " + data.size() / opTime);
        return data;
    }

    private void writeBlock(Log<Long, Integer> log, int blockSize, int partition, long ttl, String note) throws DatabaseGenericException {
        long opStart = System.currentTimeMillis();
        for (long i = (long) partition * blockSize; i < (long) (partition + 1) * blockSize; i++) {
            log.add(i, ttl, null);
        }
        long opTime = System.currentTimeMillis() - opStart;
        System.out.println(" --- WRITING " + blockSize + " records ---");
        System.out.println(" Note: " + note);
        System.out.println(" Total time, ms = " + opTime);
        if (opTime ==0) opTime = 1;
        System.out.println(" TPMS = " + blockSize / opTime);
        System.out.println(" TPS = " + (blockSize * 1000L) / opTime);
    }

    private Collection<LogItem<Long, Integer>> readBlock(Log<Long, Integer> log, int blockSize, int partition, long ttl, String note) throws DatabaseGenericException {

        Collection<LogItem<Long, Integer>> data = new LinkedList<>();

        long opStart = System.currentTimeMillis();
        for (long i = (long) partition * blockSize; i < (long) (partition + 1) * blockSize; i++) {
            var item = log.get(i);
            if (item != null) data.add(item);
        }
        long opTime = System.currentTimeMillis() - opStart;
        System.out.println(" --- READING " + blockSize + " records ---");
        System.out.println(" Note: " + note);
        System.out.println(" Total time, ms = " + opTime);
        System.out.println(" Data read, records = " + data.size());
        if (opTime ==0) opTime = 1;
        System.out.println(" TPMS = " + blockSize / opTime);
        System.out.println(" TPS = " + (blockSize * 1000L) / opTime);
        System.out.println(" Records found / ms " + data.size() / opTime);

        return data;
    }

}
