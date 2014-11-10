package com.ashwinikd.ds.mmqueue;

/**
 * Element which can be stored in a Memory Mapped Queue
 *
 * @author Ashwini Dhekane[ashwini@ashwinidhekane.com]
 *
 * @since 1.0
 */
public abstract class MemoryMappedElement {
    /**
     * Size of element in bytes
     * @since 1.0
     */
    protected static final int SIZE = -1;

    /**
     * Convert this object into byte array.
     *
     * @return the byte array representing this object
     * @since 1.0
     */
    public abstract byte[] getBytes();

    /**
     * Get the size of this object in bytes
     *
     * @return size in number of bytes
     * @since 1.0
     */
    public static final int getSize() {
        return SIZE;
    }
}
