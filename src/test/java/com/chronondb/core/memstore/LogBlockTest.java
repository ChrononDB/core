package com.chronondb.core.memstore;

import com.chronondb.core.exception.DatabaseGenericException;
import com.chronondb.core.properties.DefaultLogProperties;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.LinkedList;

public class LogBlockTest {

    @Test
    public void testCalculateBlockId() {
        Assert.assertEquals(LogBlock.calculateBlockId(0, 10), 0);
        Assert.assertEquals(LogBlock.calculateBlockId(9, 10), 0);
        Assert.assertEquals(LogBlock.calculateBlockId(10, 10), 1);
        Assert.assertEquals(LogBlock.calculateBlockId(100, 10), 10);
        Assert.assertEquals(LogBlock.calculateBlockId(109, 10), 10);
        Assert.assertEquals(LogBlock.calculateBlockId(110, 10), 11);
        Assert.assertEquals(LogBlock.calculateBlockId(0, 11), 0);
        Assert.assertEquals(LogBlock.calculateBlockId(9, 11), 0);
        Assert.assertEquals(LogBlock.calculateBlockId(10, 11), 0);
        Assert.assertEquals(LogBlock.calculateBlockId(11, 11), 1);
        Assert.assertEquals(LogBlock.calculateBlockId(100, 11), 9);
        Assert.assertEquals(LogBlock.calculateBlockId(109, 11), 9);
        Assert.assertEquals(LogBlock.calculateBlockId(110, 11), 10);
        Assert.assertEquals(LogBlock.calculateBlockId(111, 11), 10);
        Assert.assertEquals(LogBlock.calculateBlockId(1233, 1234), 0);
        Assert.assertEquals(LogBlock.calculateBlockId(1234, 1234), 1);
    }

    @Test
    public void testCalculateBlockStart() {
        Assert.assertEquals(LogBlock.calculateBlockStart(0, 10), 0);
        Assert.assertEquals(LogBlock.calculateBlockStart(9, 10), 90);
        Assert.assertEquals(LogBlock.calculateBlockStart(0, 11), 0);
        Assert.assertEquals(LogBlock.calculateBlockStart(9, 11), 99);
        Assert.assertEquals(LogBlock.calculateBlockStart(1233, 1234), 1521522);
    }

    @Test
    public void testCalculateBlockEnd() {
        Assert.assertEquals(LogBlock.calculateBlockEnd(0, 10), 9);
        Assert.assertEquals(LogBlock.calculateBlockEnd(9, 10), 99);
        Assert.assertEquals(LogBlock.calculateBlockEnd(0, 11), 10);
        Assert.assertEquals(LogBlock.calculateBlockEnd(9, 11), 109);
        Assert.assertEquals(LogBlock.calculateBlockEnd(1233, 1234), 1522755);
    }

    @Test
    public void linearReadEraseVacuumTest() throws InterruptedException, DatabaseGenericException {
        // one-thread block read-erase-vacuum

        Log<Long, Integer> log = new Log<Long, Integer>(
                new DefaultLogProperties()
                        .setBlockSize(10)
                        .setBlockVacuumDelayBlocksize(1)
                        .setBlockVacuumDelayMs(200)
        );

        int CHUNK_SIZE = 1000 * 1000;

        long opStart = System.currentTimeMillis();
        long opTime = System.currentTimeMillis() - opStart;

        // READ BLOCK
        var data = readRange(log, Long.MIN_VALUE, Long.MAX_VALUE);
        Assert.assertEquals(data.size(), 0);

        // WRITE
        writeChunk(log, CHUNK_SIZE, 0, Long.MAX_VALUE);

        // VACUUM
        var res = log.vacuum();
        Assert.assertEquals(res, 0); // must be protected still by timeout

        // READ BLOCK
        data = readRange(log, Long.MIN_VALUE, Long.MAX_VALUE);
        Assert.assertEquals(data.size(), CHUNK_SIZE);

        // ERASE BLOCK
        eraseChunk(log, CHUNK_SIZE, 0);

        // READ BLOCK
        data = readRange(log, Long.MIN_VALUE, Long.MAX_VALUE);
        Assert.assertEquals(data.size(), 0);

        Thread.sleep(201); // blocks must expire

        // VACUUM AGAIN
        res = log.vacuum();
        Assert.assertTrue(res > 10);

        // READ BLOCK
        data = readRange(log, Long.MIN_VALUE, Long.MAX_VALUE);
        Assert.assertEquals(data.size(), 0);

    }

    @Test
    public void linearBlockRotationTest() throws InterruptedException, DatabaseGenericException {
        // one-thread internal block rotation consistency test

        Log<Long, Integer> log = new Log<Long, Integer>(
                new DefaultLogProperties().setBlockSize(1)
                        .setBlockVacuumDelayMs(0)
                        .setBlockVacuumDelayBlocksize(0)
        );

        int CHUNK_SIZE=1000000;

        writeChunk(log, CHUNK_SIZE, 0, Long.MAX_VALUE);

        var data = readRange(log, Long.MIN_VALUE, Long.MAX_VALUE);
        Assert.assertEquals(data.size(), CHUNK_SIZE);

        eraseChunk(log, CHUNK_SIZE, 0);

        data = readRange(log, Long.MIN_VALUE, Long.MAX_VALUE);
        Assert.assertEquals(data.size(), 0);

        Assert.assertTrue(log.vacuum() > 50);

        data = readRange(log, Long.MIN_VALUE, Long.MAX_VALUE);
        Assert.assertEquals(data.size(), 0);

    }

    @Test
    public void linearLongChainSwapTest() throws InterruptedException, DatabaseGenericException {
        // one-thread internal block rotation consistency test with 1 record per block situation

        Log<Long, Integer> log = new Log<Long, Integer>(new DefaultLogProperties().setBlockSize(1));

        int CHAIN_SIZE = 1000;

        for (int i = 0; i < CHAIN_SIZE; i++) {
            log.add((long) i, Long.MAX_VALUE, null);
            Thread.sleep(1);
        }

        var data = readRange(log, Long.MIN_VALUE, Long.MAX_VALUE);
        Assert.assertEquals(data.size(), CHAIN_SIZE);

        eraseChunk(log, CHAIN_SIZE, 0);

        data = readRange(log, Long.MIN_VALUE, Long.MAX_VALUE);
        Assert.assertEquals(data.size(), 0);

        Assert.assertTrue(log.vacuum() > 50);

        data = readRange(log, Long.MIN_VALUE, Long.MAX_VALUE);
        Assert.assertEquals(data.size(), 0);

    }

    @Test
    public void linearSimpleBlockTTLAndVacuumTest() throws InterruptedException, DatabaseGenericException {
        // one-threaded TTL test

        Log<Long, Integer> log = new Log<Long, Integer>(
                new DefaultLogProperties()
                        .setBlockSize(10)
                        .setBlockVacuumDelayBlocksize(0)
                        .setBlockVacuumDelayMs(0)
        );

        int CHUNK_SIZE = 1000 * 1000;

        // WRITE LONG TTL
        writeChunk(log, CHUNK_SIZE, 0, Long.MAX_VALUE);

        // WRITE SHORT TTL
        writeChunk(log, CHUNK_SIZE, 1, System.currentTimeMillis() + 500);

        // READ ALL
        var data = readRange(log, Long.MIN_VALUE, Long.MAX_VALUE);
        Assert.assertEquals(data.size(), 2*CHUNK_SIZE);

        // WAIT TTL
        Thread.sleep(501);

        // READ BLOCK
        data = readRange(log, Long.MIN_VALUE, Long.MAX_VALUE);
        Assert.assertEquals(data.size(), CHUNK_SIZE);

        // VACUUM
        Assert.assertTrue(log.vacuum() > 10);

        // VACUUM
        Assert.assertEquals(log.vacuum(), 0);

        // READ BLOCK
        data = readRange(log, Long.MIN_VALUE, Long.MAX_VALUE);
        Assert.assertEquals(data.size(), CHUNK_SIZE);

        // ERASE BLOCK
        eraseChunk(log, CHUNK_SIZE, 0);

        // READ BLOCK
        data = readRange(log, Long.MIN_VALUE, Long.MAX_VALUE);
        Assert.assertEquals(data.size(), 0);

        // VACUUM
        Assert.assertTrue(log.vacuum() > 10);

        // VACUUM
        Assert.assertEquals(log.vacuum(), 0);

        // READ BLOCK
        data = readRange(log, Long.MIN_VALUE, Long.MAX_VALUE);
        Assert.assertEquals(data.size(), 0);

    }

    private void eraseChunk(Log<Long, Integer> log, int chunkSize, int indexPrefix) throws DatabaseGenericException {
        for (long i = (long) indexPrefix * chunkSize; i < (long) (indexPrefix + 1) * chunkSize; i++) {
            log.remove(i);
        }
    }

    private Collection<LogItem<Long, Integer>> readRange(Log<Long, Integer> log, long rangeStart, long rangeEnd) throws DatabaseGenericException {
        return log.get(rangeStart, rangeEnd);
    }

    private void writeChunk(Log<Long, Integer> log, int chunkSize, int indexPrefix, long ttl) throws DatabaseGenericException {
        for (long i = (long) indexPrefix * chunkSize; i < (long) (indexPrefix + 1) * chunkSize; i++) {
            log.add(i, ttl, null);
        }
    }

    private Collection<LogItem<Long, Integer>> readChunk(Log<Long, Integer> log, int chunkSize, int indexPrefix) throws DatabaseGenericException {

        Collection<LogItem<Long, Integer>> data = new LinkedList<>();

        for (long i = (long) indexPrefix * chunkSize; i < (long) (indexPrefix + 1) * chunkSize; i++) {
            var item = log.get(i);
            if (item != null) data.add(item);
        }

        return data;
    }



}
