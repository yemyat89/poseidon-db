package com.poseidon.db.utils;

import java.util.LinkedHashMap;
import java.util.Map;

public class BlockCache <K, V> extends LinkedHashMap<K, V>
{
    private static final long serialVersionUID = 3997456039742521900L;
	private final int capacity_;

    public BlockCache(int capacity)
    {
        super(capacity + 1, 1.1f, true);
        capacity_ = capacity;
    }
    
    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> entry)
    {
        return (size() > capacity_);
    }
}