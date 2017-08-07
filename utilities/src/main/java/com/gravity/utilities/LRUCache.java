package com.gravity.utilities;

import java.util.*;

public class LRUCache<A, B> extends LinkedHashMap<A, B> {
    /**
     * http://stackoverflow.com/questions/221525/how-would-you-implement-an-lru-cache-in-java-6
     */
    private static final long serialVersionUID = 1L;
    private final int maxEntries;
    private final long memoryLimit;

    public LRUCache(final int maxEntries, final long memoryLimit) {
        super(maxEntries + 1, 1.0f, true);
        System.out.println("Creating LRUCache with " + maxEntries + " max entries, with a memory limit of " + memoryLimit + " bytes");
        this.maxEntries = maxEntries;
        this.memoryLimit = memoryLimit;
    }

    // returns true if we're within memory limits, false if we're outside of our memory bounds
    private boolean checkMemory() {
        return (Runtime.getRuntime().freeMemory() > memoryLimit);
    }

    public void emergencyFreeSpace() {

    }

    /**
     * Returns <tt>true</tt> if this <code>LruCache</code> has more entries than the maximum specified when it was
     * created.
     *
     * <p>
     * This method <em>does not</em> modify the underlying <code>Map</code>; it relies on the implementation of
     * <code>LinkedHashMap</code> to do that, but that behavior is documented in the JavaDoc for
     * <code>LinkedHashMap</code>.
     * </p>
     *
     * @param eldest
     *            the <code>Entry</code> in question; this implementation doesn't care what it is, since the
     *            implementation is only dependent on the size of the cache
     * @return <tt>true</tt> if the oldest
     * @see java.util.LinkedHashMap#removeEldestEntry(Map.Entry)
     */
    @Override
    protected boolean removeEldestEntry(final Map.Entry<A, B> eldest) {
        //return (super.size() > maxEntries) || !checkMemory();
        return (super.size() > maxEntries);
    }
}
