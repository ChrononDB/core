package com.chronondb.core.memstore;

import com.chronondb.core.ItemRepository;
import com.chronondb.core.exception.OverloadException;
import com.chronondb.core.exception.DatabaseGenericException;
import com.chronondb.core.properties.LogProperties;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Session Log
 *
 * @param <K> record Id type
 * @param <V> Payload type
 */
public class Log<K,V> implements ItemRepository<K, V> {
    /**
     * Block chain head.
     */
    private volatile LogBlock<K, V> head;

    /**
     * Current block
     */
    private volatile LogBlock<K, V> current;

    /**
     * Session index to navigate storage block. Key - session Id, value - registration date.
     * - Value must NOT be a block itself, 'cause this may impact memory management. I created code in a way that VACUUM
     * will clean-up the index also, but there is another reason
     * - Value must NOT be a block or block id, but exact registration time, 'cause otherwise this will impact bucket
     * selection mechanism and will require bucket search, which is much slower.
     */
    private final Map<K, Long> sessionIndex = new ConcurrentHashMap<>();

    /**
     * Block index to find a block in memory by Id. Here is the downside of in-memory with JMM
     */
    private final Map<Long, LogBlock<K, V>> blockIndex = new ConcurrentHashMap<>();

    private final LogProperties settings;

    /**
     * Lock to rotate the block. Block rotation will not impact DELETE, GET or FLUSH, but ADD will be paused.
     * <p>
     * If block rotation will be started without this sync in parallel, some data may be lost. Another way to avoid data
     * corruption without synchronization overhead is to implement proper caching while block is rotating OR block graph
     * instead of block chain with a merge at the VACUUM stage.
     * <p>
     * Anyway, block rotation is a jet fast and happens once in blockSize ms.
     */
    private final ReentrantLock newBlockMutex = new ReentrantLock();

    /**
     * Lock to prevent multiple VACUUM cleans in parallel. No any harm by the way, and moreover I considered a waves of
     * VACUUMERs, when on is in the end of chain, another one can start. But, to be honest, I would prefer to parallelize
     * vacuuming instead, this is easy.
     */
    private final ReentrantLock vacuumMutex = new ReentrantLock();

    /**
     * Block size required for time2block mapping. Why this is here? Well, I'm seriously considered dynamic block size
     */
    private final int blockSize;

    /**
     * Init Log
     *
     * @param settings Settings, see type for details
     */
    public Log(LogProperties settings) {
        // no way to parse here property file or read Properties. Do it outside
        this.settings = settings;
        // cache it
        this.blockSize = settings.getBlockSize();

        // Init first block
        var initBlock = new LogBlock<K, V>(settings);

        // write index and references
        blockIndex.put(initBlock.getBlockId(), initBlock);
        head = initBlock;
        current = initBlock;
    }

    /**
     * Add session to log
     *
     * @param itemId       unique key representing a user playback session
     * @param expiryTimeMillis absolute expiration time in milliseconds, must be in the future
     * @throws DatabaseGenericException If interrupted while locking
     */
    @Override
    public void add(K itemId, long expiryTimeMillis, V payload) throws DatabaseGenericException {
        // HAPPY PATH: let's streamline happy path, this will save us some time
        if (tryOptimisticAdd(itemId, expiryTimeMillis, payload)) return;

        // Okay, current block is from the past, time to roll it over. Most tricky part, we need synchronization.
        try {
            // get lock, if timed out - return exception to slow down
            if (!newBlockMutex.tryLock(settings.getLockThreshold(), TimeUnit.MILLISECONDS))
                throw new OverloadException("FATAL: Timeout on lock to switch block, system locked or overloaded");
            // Okay, we've got the lock, let's check again, maybe it was already rotated to what we need
            if (tryOptimisticAdd(itemId, expiryTimeMillis, payload)) return;

            // Hell, we still there... Rotate the block!
            rotateTheBlockUnsafe();

            /* Force add to new a block at the block creation time which should be considered as register  time.
            Here is the problem, actually even 2
            - if GC will hit you at the block generation time and block is small, you'll miss newly generated block
            - if block was generated at the last nanoseconds, new write will miss the block
            So, block gen time must me reg time for the record which triggered block rotation
            */
            sessionIndex.put(itemId, current.getBlockGen());
            current.add(itemId, current.getBlockGen(), expiryTimeMillis, payload);

        } catch (InterruptedException e) {
            throw new DatabaseGenericException("Command Thread interrupted!");
        } finally {
            if (newBlockMutex.isHeldByCurrentThread()) newBlockMutex.unlock();
        }
    }

    /**
     * Generate and append a new block. NOT thread-safe. Invoker is responsible for synchronization.
     */
    private void rotateTheBlockUnsafe() {
        var newBlock = new LogBlock<K, V>(settings);
        current.setNextBlockUnsafe(newBlock);
        current = newBlock;
        blockIndex.put(newBlock.getBlockId(), newBlock);
    }

    /**
     * Attempts to insert data into current block.
     *
     * @param sessionKey       Session key
     * @param expiryTimeMillis TTL
     * @return Is data written
     */
    private boolean tryOptimisticAdd(K sessionKey, long expiryTimeMillis, V payload) {
        // NEVER EVER pass command time or fix registerTime here. It MUST be fresh to match the current block.

        // We don't need any synchronization here, we copy the reference and if block is good - write it
        LogBlock<K, V> targetBlock = current;
        // create timemark
        long registerTime = System.currentTimeMillis();

        // if block good - write it to the block and index!
        if (targetBlock.isBlockGoodFor(registerTime)) {
            sessionIndex.put(sessionKey, registerTime);
            targetBlock.add(sessionKey, registerTime, expiryTimeMillis, payload);
            return true;
        }

        // OKAY, we still here, block is not good, is it from the future?
        if (targetBlock.getBlockStart() > registerTime)
            throw new IllegalStateException("Block from the future detected! Logic error");

        return false;
    }

    /**
     * Remove session data from Log
     *
     * @param sessionKey unique key representing a user playback session
     * @throws DatabaseGenericException On repo internal error
     */
    @Override
    public void remove(K sessionKey) throws DatabaseGenericException {
        // get registration time and deregister from index
        Long registerTime = sessionIndex.remove(sessionKey);
        // nothing in index means nothing in data
        if (registerTime == null) return;
        // get target block
        var targetBlock = blockIndex.get(LogBlock.calculateBlockId(registerTime, this.blockSize));
        // if block alive - remove session
        if (targetBlock != null) targetBlock.remove(sessionKey, registerTime);
    }

    /**
     * Returns data for specified period
     *
     * @param startTimeMillis start of the time range in milliseconds
     * @param endTimeMillis end of the time range in milliseconds
     * @return Log items
     * @throws DatabaseGenericException On repo internal error
     */
    @Override
    public Collection<LogItem<K, V>> get(long startTimeMillis, long endTimeMillis) throws DatabaseGenericException {
        // Okay, we have another code duplicate with FLUSH. Unfortunately, any my attempts to move this to the function
        // do more complications than resolves. So be it

        long commandTime = System.currentTimeMillis();

        // no any sense to query beyond command time
        endTimeMillis = Math.min(endTimeMillis, commandTime);
        var headBlock = head;
        // no any sense to query before log's oldest block
        startTimeMillis = Math.max(startTimeMillis, headBlock.getBlockStart());

        var startBlockId = LogBlock.calculateBlockId(startTimeMillis, this.blockSize);
        var endBlockId = LogBlock.calculateBlockId(endTimeMillis, this.blockSize);

        // Yes, like I said, I would prefer something tree-based rather than plain map as block index.
        // But for now it is what it is
        var targetBlock = findOldestAvailableBlock(startBlockId, endBlockId);

        var result = new LinkedList<LogItem<K,V>>();

        // walk through the block chain 'till the end OR end block
        while (targetBlock != null && targetBlock.getBlockId() <= endBlockId) {
            if (!targetBlock.isObsolete())
                result.addAll(targetBlock.get(startTimeMillis, endTimeMillis, commandTime));
            targetBlock = targetBlock.getNextBlock();
        }

        return result;
    }

    /**
     * Returns specific record by Id
     *
     * @param itemId Item Id to get
     * @return Log item or null, if not found
     * @throws DatabaseGenericException On repo internal error
     */
    @Override
    public LogItem<K, V> get(K itemId) throws DatabaseGenericException {
        var time = sessionIndex.get(itemId);
        if (time == null) return null;

        var block = blockIndex.get(LogBlock.calculateBlockId(time, blockSize));
        if (block == null) {
            sessionIndex.remove(itemId);
            return null;
        }

        var item = block.get(itemId, time);
        if (item == null) {
            sessionIndex.remove(itemId);
            return null;
        }

        return item;
    }

    /**
     * Search for oldest available block in a range
     *
     * @param startBlockId Start block Id (inclusive)
     * @param endBlockId End block Id (inclusive)
     * @return Oldest block in a range or null, if not found
     */
    private LogBlock<K, V> findOldestAvailableBlock(long startBlockId, long endBlockId) {

        for (long id = startBlockId; id <= endBlockId; id++) {
            var candidate = blockIndex.get(id);
            // We SHOULD not to check here for obsolete block. Scan through chain is cheaper than through index and
            // I can prove it.
            if (candidate != null) return candidate;
        }

        return null;
    }

    /**
     * Flushes records created in this timerange
     *
     * @param startTimeMillis start of the time range in milliseconds
     * @param endTimeMillis end of the time range in milliseconds
     * @throws DatabaseGenericException On repo internal error
     */
    @Override
    public void flush(long startTimeMillis, long endTimeMillis) throws DatabaseGenericException {
        // Okay, we have another code duplicate with GET. Unfortunately, any my attempts to move this to the function
        // do more complications than resolves. So be it

        long commandTime = System.currentTimeMillis();

        // no any sense to query beyond command time
        endTimeMillis = Math.min(endTimeMillis, commandTime);
        var headBlock = head;
        // no any sense to query before log's oldest block
        startTimeMillis = Math.max(startTimeMillis, headBlock.getBlockStart());

        var startBlockId = LogBlock.calculateBlockId(startTimeMillis, this.blockSize);
        var endBlockId = LogBlock.calculateBlockId(endTimeMillis, this.blockSize);

        // Yes, like I said, I would prefer something tree-based rather than plain map as block index.
        // But for now it is what it is
        var targetBlock = findOldestAvailableBlock(startBlockId, endBlockId);

        // walk through the block chain 'till the end OR end block
        while (targetBlock != null && targetBlock.getBlockId() <= endBlockId) {
            if (!targetBlock.isObsolete())
                targetBlock.flush(startTimeMillis, endTimeMillis, commandTime, sessionIndex);
            targetBlock = targetBlock.getNextBlock();
        }
    }

    /**
     * Vacuums blocks and items in blocks
     * <p>
     * Method MUST be thread-safe otherwise this will damage block chain.
     * Nevertheless, VACUUM does not affect other operations as well as block chain change.
     *
     * @return Blocks removed
     */
    protected int vacuum() {
        int counter = 0;

        try {
            if (!vacuumMutex.tryLock()) return 0;

            var prev = head.getNextBlock();

            // no enough block to clean
            if (prev == null || prev.getNextBlock() == null) return 0;

            /* Since I'm the only process who can change block chain except rotation - I don't care about concurrency
            if I don't operate in the end of the chain and can guarantee that I have enough blocks behind.

            Since our next block reference changes from null to value only - stale data will INCREASE safety.

            Since I don't corrupt blocks itself, my changes to a chain will not corrupt existing threads */

            while (prev != null && prev.getNextBlock() != null && prev.getNextBlock().getNextBlock() != null && prev.getNextBlock().getNextBlock() != current) {
                // okay, if we here, then we just left mutable part of the chain (end) behind.
                var targetBlock = prev.getNextBlock();
                // if block is not vacuumable this is guaranteed that there are no vacuumable blocks after
                if (!targetBlock.isVacuumable()) return counter;
                // Vacuum block. Yes, yes, it can be already obsolete. But VACUUM can be long, I'd double check
                targetBlock.vacuumUnsafe(sessionIndex);

                if (targetBlock.isObsolete()) {
                    // remove obsolete block from index, no harm in it even if we will stop here
                    blockIndex.remove(targetBlock.getBlockId());
                    // exclude obsolete block from the chain
                    prev.setNextBlockUnsafe(targetBlock.getNextBlock());
                    counter++;
                } else prev = prev.getNextBlock(); // if block is not obsolete, go to the next block
            }

            // Time to care about the head itself. Again, we are the only one who change the chain in this part and we want
            // to be sure, this is why all these getNext().getNext()
            // a bit overprotective and wordy, yes, yes
            var targetBlock = head;
            if (targetBlock.getNextBlock() != null && targetBlock.getNextBlock().getNextBlock() != null && targetBlock.getNextBlock().getNextBlock() != current && targetBlock.isVacuumable()) {
                targetBlock.vacuumUnsafe(sessionIndex);
                if (targetBlock.isObsolete()) head = targetBlock.getNextBlock();
            }
            return counter;

        } finally {
            if (vacuumMutex.isHeldByCurrentThread()) vacuumMutex.unlock();
        }

    }
}
