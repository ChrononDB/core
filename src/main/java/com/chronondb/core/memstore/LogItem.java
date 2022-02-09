package com.chronondb.core.memstore;

/**
 * Log item object
 *
 * @param <K> Item Id key
 * @param <V> Item payload
 */
public class LogItem<K, V> {
    private K id;
    private long registerTime;
    private long ttl;
    private V payLoad;

    /**
     * Constructor
     *
     * @param id Id
     * @param registerTime Register time
     * @param ttl TTK, absolute
     * @param payLoad Payload
     */
    public LogItem(K id, long registerTime, long ttl, V payLoad) {
        this.id = id;
        this.registerTime = registerTime;
        this.ttl = ttl;
        this.payLoad = payLoad;
    }

    /**
     * Returns item Id
     *
     * @return Item Id
     */
    public K getId() {
        return id;
    }

    /**
     * Return log register time
     *
     * @return register time
     */
    public long getRegisterTime() {
        return registerTime;
    }

    /**
     * Returns item TTL
     *
     * @return TTL, absolute
     */
    public long getTtl() {
        return ttl;
    }

    /**
     * Returns payload
     *
     * @return Payload
     */
    public V getPayLoad() {
        return payLoad;
    }
}
