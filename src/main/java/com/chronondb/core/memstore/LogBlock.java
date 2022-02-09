package com.chronondb.core.memstore;

import com.chronondb.core.properties.LogProperties;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Log Block, for now fixed size only.
 *
 * @param <K> recordId type
 * @param <V> Payload type
 */
public class LogBlock<K, V> {

    /**
     * Block id which is generated from the time. If blockSize is 100, then time 105 belongs to block id 2
     */
    private final long blockId;

    /**
     * Block time start, ms. Derived from block id, obviously. Block 2 of size 100 will handle time starting 100ms
     */
    private final long blockStart;

    /**
     * Block time start, ms. Derived from block id, obviously. Block 2 of size 100 will handle time till 199ms
     */
    private final long blockEnd;

    /**
     * Block generation time. For debug, statistics and to resolve race conditions
     */
    private final long blockGen;

    /**
     * When block can be vacuumed, absolute time
     */
    private final long vacuumableAfter;

    /**
     * Block size. Now only static size supported, but dynamic block size is also possible with proper
     * - calculateBlockId()
     * - calculateBlockStart()
     * - calculateBlockEnd()
     */
    private final int blockSize;

    /**
     * Buckets, per ms. We are NOT going to change this beyond constructor, so no changes - no problems
     * Key is session Id, value is TTL
     */
    private final ArrayList<Map<K, LogItem<K, V>>> buckets;

    /**
     * We have a block chain to speed-up navigation after the first block found and to skip a dead blocks.
     * <p>
     * Algorithm built in a way that any changes to chain are safe. I'm using the fact that objects are still alive if
     * active thread have a reference. So, GC is kinda keeper of an excluded blocks 'till threads will process them.
     * Outside of Java this will require additional block management code, so, god bless GC.
     * <p>
     * I don't care about synchronization a lot, 'cause in the worst case scenario thread will just scan dead block.
     */
    private volatile LogBlock<K, V> nextBlock = null;

    /**
     * Block marked as obsolete will be excluded from read operations. If block marked as obsolete, this is permanent.
     * <p>
     * It's an optimization, so I don't care much about any sync, some threads may still scan obsolete block until
     * change is propagated, we have another line of sync at the bucket level, where sessions added/removed
     */
    private volatile boolean obsolete = false;

    /**
     * Mark block as obsolete to exclude it from any read.
     * <p>
     * Idempotent, thread-safe. Must not be used by any logic except VACUUM
     */
    protected void markObsolete() {
        this.obsolete = true;
    }

    /**
     * Block constructor
     *
     * @param settings Initial settings, see interface for details
     */
    protected LogBlock(LogProperties settings) {

        // maybe you read it through RMI, don't you? I'll cache in variable just in case
        this.blockSize = settings.getBlockSize();

        // init block header
        this.blockGen = System.currentTimeMillis();
        this.blockId = calculateBlockId(this.blockGen, blockSize);
        this.blockStart = calculateBlockStart(this.blockId, blockSize);
        this.blockEnd = calculateBlockEnd(this.blockId, blockSize);
        this.vacuumableAfter = this.blockEnd + Math.max(
                settings.getBlockVacuumDelayInMs(),
                blockSize * settings.getBlockVacuumDelayInBlockSize()
        );

        // Init buckets. Since no changes to buckets holder after constructor - it's a thread-safe
        this.buckets = new ArrayList<>(blockSize);
        // Create thread-safe buckets
        for (int i = 0; i < blockSize; i++) {
            buckets.add(new ConcurrentHashMap<K, LogItem<K, V>>());
        }
    }

    /**
     * Calculate last millisecond of a block by blockId
     *
     * @param blockId   Block Id
     * @param blockSize Block size
     * @return last millisecond of a block
     */
    protected static long calculateBlockEnd(long blockId, int blockSize) {
        return (blockId + 1) * blockSize - 1;
    }

    /**
     * Calculate first millisecond of a block by blockId
     *
     * @param blockId   Block Id
     * @param blockSize Block size
     * @return first millisecond of a block
     */
    protected static long calculateBlockStart(long blockId, int blockSize) {
        return blockId * blockSize;
    }

    /**
     * Get block Id by time mark
     *
     * @param time      Time mark
     * @param blockSize Block size
     * @return Target block Id
     */
    protected static long calculateBlockId(long time, int blockSize) {
        // okay, okay, but this MUST be separate method. It is simple just for now.
        return time / blockSize;
    }

    /**
     * Is this block marked as obsolete
     *
     * @return Is this block marked as obsolete
     */
    protected boolean isObsolete() {
        return obsolete;
    }

    /**
     * Adds a block chain continuation.
     * <p>
     * WARNING: Invoker MUST care about proper synchronization when adding new block to the chain. This method itself
     * is NOT thread safe
     * <p>
     * This method ultimately must not be used by anything other than block rotation mechanism or VACUUM.
     * VACUUM must never process last block, see header for vacuum delay.
     *
     * @param nextBlock Next block to append
     * @return false, if next block is null
     */
    protected boolean setNextBlockUnsafe(LogBlock<K, V> nextBlock) {
        /* You can't zero next block once it is set. Block chain is somewhat permanent, except vaccum shortcuts.
         If some blocks are shortcuted, they still have a reference to the chain continuation. So is some thread stays
         on a shortcuted block - it is still ok

         No sync is needed cause method itself must be invoked in a thread-safe manner
         */
        if (nextBlock == null) return false;

        this.nextBlock = nextBlock;

        return true;
    }

    /**
     * Vacuums the block.
     * <p>
     * There is no harm in running this method in parallel, just the performance degradation.
     *
     * @param globalItemIndex Global item index to clean-up
     * @return Is block agreed to vacuum (delay passed) and did a vacuum
     */
    protected boolean vacuumUnsafe(Map<K, Long> globalItemIndex) {
        // TODO: Implement more advanced way to clean up index through some delayed messaging
        // is block can be vacuumed? Extra layer of protection to NOT vacuum last block
        // we have a one-way change on nextBlock and even if we will miss flag now, block will be vacuumed later
        if (!isVacuumable() || this.nextBlock == null) return false;

        // we need this cache var for optimization only, it's not for any consistency purpose
        long time = System.currentTimeMillis();

        // Way to detect empty block to remove it from the chain. Empty block is not a big time waster 'cause we can
        // compact it on the fly, thanks to in-memory vs disk. Nevertheless, in will slow down chain navigation.
        boolean emptyBlock = true;

        // iterate through buckets
        for (var bucket : buckets) {
            // iterate through sessions, thread-safe iterator behind
            for (var entry : bucket.entrySet()) {
                // Our record was not deleted yet?

                if (entry == null || entry.getValue() == null) continue;

                // if it is expired - remove from bucket and index, otherwise block is not empty.
                if (time > entry.getValue().getTtl()) {
                    // safe by definition, ConcurrentHashMap under the hood
                    bucket.remove(entry.getKey());
                    globalItemIndex.remove(entry.getKey());
                } else emptyBlock = false;
            }
        }

        if (emptyBlock) markObsolete();

        return true;
    }

    /**
     * Is block vacuumable?
     *
     * @return Block vacuum delay passed and we have next block
     */
    protected boolean isVacuumable() {
        return System.currentTimeMillis() > this.vacuumableAfter && this.nextBlock != null;
    }

    /**
     * Calculate bucket index by time
     *
     * @param registerTime Time
     * @return Target bucket index
     */
    int calculateBucketIndex(long registerTime) {
        return (int) (registerTime % blockSize);
    }

    /**
     * Next block in a chain.
     * <p>
     * It's thread-safe due to nature how chain is processed.
     * If next block was shortcuted - such block stil valid for traversal.
     * If next block was added - some GET may not see it, till it fully registered, which is totally ok
     *
     * @return Next block in a chain
     */
    protected LogBlock<K, V> getNextBlock() {
        return nextBlock;
    }

    /**
     * Add session info to block. Absolutely thread-safe due to underlying collection
     * <p>
     * WARNING: LogBlock is not responsible for any SessionIndex updates for ADD. Invoker MUST register  session in the
     * SessionIndex with the proper RegisterTime BEFORE pushing data to block
     *
     * @param itemId       Session key
     * @param registerTime Time at which session registered to choose proper bucket.
     * @param expiryTime   TTL, absolute time
     * @param payload      Payload
     */
    protected void add(K itemId, long registerTime, long expiryTime, V payload) {
        // yes, this is thread safe
        if (!isBlockGoodFor(registerTime))
            throw new IllegalStateException("Attempt to register in a wrong block!");

        // register session in Log
        buckets.get(calculateBucketIndex(registerTime)).put(itemId, new LogItem<>(itemId, registerTime, expiryTime, payload));
    }

    /**
     * Remove session from the Log, if index failed (no register time known). Triggers block-wide search.
     * <p>
     * WARNING: LogBlock is not responsible for any SessionIndex updates for REMOVE. Invoker MUST deregister session
     * from the SessionIndex with the proper RegisterTime BEFORE removing data from block.
     * <p>
     * WARNING: System is NOT supporting multiple sessions with the same sessionId. Search will stop on first item.
     *
     * @param sessionKey Session key
     * @return Did we actually found an item. If block obsolete, no search conducted
     */
    protected boolean remove(K sessionKey) {
        // If the whole block is obsolete - do nothing.
        // We don't care about any sync because is this is obsolete, but we miss it - well, just another hit to bucket
        // This optimization, by the way, mess up results for obsolete block
        if (isObsolete()) return true;

        // I hate to do this, but if index failed for some reason, you can run wide search
        for (var bucket : buckets) {
            if (bucket.remove(sessionKey) != null) return true;
        }

        return false;
    }

    /**
     * Remove session from the Log by index data
     *
     * @param sessionKey   Session key
     * @param registerTime Registration time
     * @return Did we actually found an item. If block obsolete, no search conducted
     */
    protected boolean remove(K sessionKey, long registerTime) {
        if (isObsolete()) return false;

        // Buckets list and bucket's maps are initialized in constructor and MUST not be touched (replaced with null etc.)
        return buckets.get(calculateBucketIndex(registerTime)).remove(sessionKey) != null;
    }

    /**
     * GET which also VACUUMs expired sessions. The idea that remove() is fast and TTL logic is already there.
     *
     * @param startTimeMillis Start time (can be outside of the block range, no problem)
     * @param endTimeMillis   End time (can be outside of the block range, no problem)
     * @param commandTime     Command time to provide consistent read results, nevertheless strong consistency is not
     *                        guaranteed, 'cause VACUUM can run concurrently and can remove some TTLs. So, if you have
     *                        records A and B with the same TTL, it is possible that one will be included and another one not.
     * @param sessionIndex    SessionIndex to do clean-up
     * @return Extracted data from the block. Due to concurrency, if you have records A and B with the same TTL, it is
     * possible that one will be included and another one not.
     */
    Collection<LogItem<K, V>> getAndClean(long startTimeMillis, long endTimeMillis, long commandTime, Map<K, Long> sessionIndex) {
        // Okay, okay, I totally understand "Dmitry, why you've duplicated the code?".
        // It's a boilerplate, so choose right method instead of passing right parameters
        var result = new LinkedList<LogItem<K, V>>();

        // A bit of protection. If block is obsolete or query time range beyond block time range - return nothing
        if (isObsolete() || startTimeMillis > endTimeMillis || startTimeMillis > blockEnd || endTimeMillis < blockStart)
            return result;

        // Fit query frame to block frame. Cut query to the buckets from the future
        long start = Math.max(blockStart, startTimeMillis);
        long end = Math.min(blockEnd, Math.min(endTimeMillis, commandTime));

        // Command time for inclusion, system for clean-ups. Think about long commands
        // I'd like to have a fresh time on each bucket, even each item to check TTL, but perfomance.
        // Let it be, VACUUM will do the rest
        long systemTime = System.currentTimeMillis();

        for (int i = calculateBucketIndex(start); i <= calculateBucketIndex(end); i++) {
            // thread safe, read only
            var bucket = buckets.get(i);
            // thread-safe iterator behind
            for (var entry : bucket.entrySet()) {
                // still not deleted? Wow!
                if (entry == null || entry.getValue() == null) continue;

                if (entry.getValue().getTtl() >= commandTime) result.add(entry.getValue());

                if (entry.getValue().getTtl() < systemTime) {
                    bucket.remove(entry.getKey());
                    sessionIndex.remove(entry.getKey());
                }
            }
        }

        return result;
    }


    /**
     * GET
     *
     * @param startTimeMillis Start time (can be outside of the block range, no problem)
     * @param endTimeMillis   End time (can be outside of the block range, no problem)
     * @param commandTime     Command time to provide consistent read results, nevertheless strong consistency is not
     *                        guaranteed, 'cause VACUUM can run concurrently and can remove some TTLs. So, if you have
     *                        records A and B with the same TTL, it is possible that one will be included and another one not.
     * @return Extracted data from the block. Due to concurrency, if you have records A and B with the same TTL, it is
     * possible that one will be included and another one not.
     */
    Collection<LogItem<K, V>> get(long startTimeMillis, long endTimeMillis, long commandTime) {
        var result = new LinkedList<LogItem<K, V>>();

        // a bit of protection
        if (isObsolete() || startTimeMillis > endTimeMillis || startTimeMillis > blockEnd || endTimeMillis < blockStart)
            return result;

        long start = Math.max(blockStart, startTimeMillis);
        long end = Math.min(blockEnd, Math.min(endTimeMillis, commandTime));

        for (int i = calculateBucketIndex(start); i <= calculateBucketIndex(end); i++) {
            // thread safe, read only
            var bucket = buckets.get(i);
            // thread-safe iterator behind
            for (var entry : bucket.entrySet()) {
                // not sure that this is needed, but don't wanna play with a concurrent behavior in this map
                // technically I must never see null entry or null value
                if (entry == null || entry.getValue() == null) continue;
                // still not deleted? Wow!
                if (entry.getValue().getTtl() >= commandTime) result.add(entry.getValue());
            }
        }

        return result;
    }

    /**
     * GET
     *
     * @param itemId Item Id
     */
    LogItem<K, V> get(K itemId, long registerTime) {
        // a bit of protection
        if (isObsolete()) return null;

        var bucket = buckets.get(calculateBucketIndex(registerTime));
        var item = bucket.get(itemId);

        if (item != null && item.getTtl() < System.currentTimeMillis()) {
            bucket.remove(itemId); // some self-cleaning, it's cheap
            item = null;
        }

        return item;
    }


    /**
     * FLUSH. I expect that FLUSH is a rare operation, so FLUSH actually scratches the data out from buckets and index,
     * not just mark it for deletion. So, FLUSH is slow, but GET is fast.
     * <p>
     * FLUSH is trying to NOT touch the data which are in range, but arrived in parallel with FLUSH execution. Still
     * there is a chance that we will remove something arrived in parallel with FLUSH the same millisecond.
     * Can be mitigated by decreasing commandTime for 1ms.
     *
     * @param startTimeMillis    Start time (can be outside of the block range, no problem)
     * @param endTimeMillis      End time (can be outside of the block range, no problem)
     * @param commandTime        Command time to not flush anything which is in range, but after command time
     * @param globalSessionIndex SessionIndex to do clean-up
     */
    protected void flush(long startTimeMillis, long endTimeMillis, long commandTime, Map<K, Long> globalSessionIndex) {
        /* WARNING: If FLUSH is used more frequently, we will need obsoleteBucketsLog which will be honored by GET and
        will be a task queue for VACUUM. */

        // a bit of protection
        if (isObsolete() || startTimeMillis > endTimeMillis || startTimeMillis > blockEnd || endTimeMillis < blockStart)
            return;

        // Fit query time frame to block time frame and limit frame my command time
        long start = Math.max(blockStart, startTimeMillis);
        // I'm NOT going to flush any records which are added AFTER flush command was issued.
        long end = Math.min(blockEnd, Math.min(endTimeMillis, commandTime));

        // Reverse order is extremely important to flush the end of period ASAP to minimize loss of data which came
        // concurrently with and after flush
        for (int i = calculateBucketIndex(end); i >= calculateBucketIndex(start); i--) {
            var targetBucket = buckets.get(i);

            // There is a chance that we will remove something arrived in parallel with FLUSH the same millisecond.
            // If we want to avoid it, versioning must be implemented
            for (var sessionKey : targetBucket.keySet()) {
                globalSessionIndex.remove(sessionKey);
                // Please DO NOT replace this with .clear(). Collection is live, you will have a race conditions.
                targetBucket.remove(sessionKey);
            }
        }
    }

    /**
     * Returns block id. Block id is generated from time in a pre-defined way
     *
     * @return block Id
     */
    protected long getBlockId() {
        return blockId;
    }

    /**
     * Get block generation time
     *
     * @return block generation time, absolute
     */
    public long getBlockGen() {
        return blockGen;
    }

    /**
     * Get block start
     *
     * @return absolute time, ms
     */
    protected long getBlockStart() {
        return blockStart;
    }

    /**
     * Get block end
     *
     * @return absolute time, ms
     */
    protected long getBlockEnd() {
        return blockEnd;
    }

    /**
     * Is this time can be registered to this log block
     *
     * @param registerTime Register time
     * @return Is this time can be registered to this log block
     */
    protected boolean isBlockGoodFor(long registerTime) {
        return registerTime >= blockStart && registerTime <= blockEnd;
    }

}
