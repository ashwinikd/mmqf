package com.ashwinikd.mmqueue;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Created by ashwini on 10/11/14.
 */
public class MemoryMappedInteger extends MemoryMappedElement {

    protected static final int SIZE = 4;
    private int n;

    public MemoryMappedInteger(int i) {
        n = i;
    }

    public MemoryMappedInteger(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.allocate(bytes.length);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        for(int i = 0; i < buffer.limit(); i++) {
            buffer.put(bytes[i]);
        }
        buffer.rewind();
        n = buffer.getInt();
    }

    @Override
    public byte[] getBytes() {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(n);
        return buffer.array();
    }

    public int get() {
        return n;
    }

    public static int getSize() {
        return SIZE;
    }
}
