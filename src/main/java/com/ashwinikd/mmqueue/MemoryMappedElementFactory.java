package com.ashwinikd.mmqueue;

/**
 * Created by ashwini on 10/11/14.
 */
public interface MemoryMappedElementFactory<T extends MemoryMappedElement> {
    public T fromBytes(byte[] bytes);
}
