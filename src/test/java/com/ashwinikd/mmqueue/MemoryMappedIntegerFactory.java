package com.ashwinikd.mmqueue;

/**
 * Created by ashwini on 10/11/14.
 */
public class MemoryMappedIntegerFactory implements MemoryMappedElementFactory<MemoryMappedInteger> {
    @Override
    public MemoryMappedInteger fromBytes(byte[] bytes) {
        return new MemoryMappedInteger(bytes);
    }
}
