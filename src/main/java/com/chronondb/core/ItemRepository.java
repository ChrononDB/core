package com.chronondb.core;

import com.chronondb.core.exception.DatabaseGenericException;
import com.chronondb.core.memstore.LogItem;

import java.util.Collection;

/**
 * Item repository interface
 *
 * @param <K> Item Id type
 * @param <V> Payload type
 */
public interface ItemRepository<K, V> {

    /**
     * Adds {@code itemId} to the tracking repository with the provided expiration time.
     *
     * @param itemId unique key
     * @param expiryTimeMillis absolute expiration time in milliseconds, must be in the future
     * @param payload Payload to keep
     * @throws DatabaseGenericException On internal error
     */
    void add(K itemId, long expiryTimeMillis, V payload) throws DatabaseGenericException;

    /**
     * Removes the tracking entry for {@code itemId}
     *
     * @param itemId unique key
     * @throws DatabaseGenericException On internal error
     */
    void remove(K itemId) throws DatabaseGenericException;


    /**
     * Returns data for specified period
     *
     * @param startTimeMillis start of the time range in milliseconds
     * @param endTimeMillis end of the time range in milliseconds
     * @return collection of active items between startTime and endTime
     * @throws DatabaseGenericException On internal error
     */
    Collection<LogItem<K,V>> get(long startTimeMillis, long endTimeMillis) throws DatabaseGenericException;

    /**
     * Returns data for specified Id
     *
     * @param itemId Item Id
     * @return Log item or null, if not found
     * @throws DatabaseGenericException On internal error
     */
    LogItem<K,V> get(K itemId) throws DatabaseGenericException;

    /**
     * Removes data for time range [startTime, endTime]
     *
     * @param startTimeMillis start of the time range in milliseconds
     * @param endTimeMillis end of the time range in milliseconds
     * @throws DatabaseGenericException On internal error
     */
    void flush(long startTimeMillis, long endTimeMillis) throws DatabaseGenericException;
}
