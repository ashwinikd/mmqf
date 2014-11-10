package com.ashwinikd.ds.mmqueue.impl;

import com.ashwinikd.ds.mmqueue.MemoryMappedElement;
import com.ashwinikd.ds.mmqueue.MemoryMappedElementFactory;
import com.ashwinikd.ds.mmqueue.MemoryMappedQueue;
import com.ashwinikd.ds.mmqueue.file.MemoryMappedQueueFile;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by ashwini on 09/11/14.
 */
public class MemoryMappedQueueImpl<T extends MemoryMappedElement> implements MemoryMappedQueue<T> {
    private AtomicInteger sequence;
    private int head;
    private int tail;
    private int size;
    private int cursor;
    private final int initialHead;
    private final int initialTail;
    private final int capacity;
    private final int slotSize;
    private final MappedByteBuffer BUFFER;
    private final MemoryMappedElementFactory<T> elementFactory;

    private static final int HEAD_POS = 0;
    private static final int HEAD_SIZ = 8;
    private static final int TAIL_POS = 8;
    private static final int TAIL_SIZ = 8;
    private static final int SIZE_POS = 16;
    private static final int SIZE_SIZ = 8;

    public MemoryMappedQueueImpl(MemoryMappedQueueFile file, MemoryMappedElementFactory<T> factory) throws IOException {
        sequence = new AtomicInteger(0);
        BUFFER = file.getDataBuffer();
        initialTail = readInt(TAIL_POS, TAIL_SIZ);
        initialHead = readInt(HEAD_POS, HEAD_SIZ);
        head = initialHead;
        tail = initialTail;
        size = readInt(SIZE_POS, SIZE_SIZ);
        capacity = file.getCapacity();
        slotSize = file.getSlotSize();
        elementFactory = factory;
    }

    private long readLong(int pos, int siz) {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < siz; i++) {
            buffer.put(BUFFER.get(pos + i));
        }
        buffer.rewind();
        return buffer.getLong();
    }

    private void writeLong(long n, int pos, int siz) {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.putLong(n);
        for (int i = 0; i < buffer.limit(); i++) {
            BUFFER.put(pos + i, buffer.get());
        }
    }

    private int readInt(int pos, int siz) {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < siz; i++) {
            buffer.put(BUFFER.get(pos + i));
        }
        buffer.rewind();
        return buffer.getInt();
    }

    private void writeInt(int n, int pos, int siz) {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(n);
        for (int i = 0; i < buffer.limit(); i++) {
            BUFFER.put(pos + i, buffer.get());
        }
    }

    @Override
    public long size() {
        return size;
    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    @Override
    public boolean isFull() {
        return size >= capacity;
    }

    @Override
    public long getCapacity() {
        return capacity;
    }

    @Override
    public void clear() throws IOException {
    }

    @Override
    public boolean enqueue(T e) throws IOException {
        int claimedSequence = sequence.incrementAndGet();
        int position = 32 +  ((initialTail + slotSize * (claimedSequence - 1)) % (capacity*slotSize));
        byte[] bytes = e.getBytes();
        for (int i = 0; i < bytes.length; i++) {
            BUFFER.put(i + position, bytes[i]);
        }
        int expectedSequence = claimedSequence - 1;
        while(cursor != expectedSequence);
        size++;
        writeLong(size, SIZE_POS, SIZE_SIZ);
        cursor = claimedSequence;
        return false;
    }

    @Override
    public T dequeue() throws IOException, NoSuchElementException {
        if (size == 0) {
            throw new NoSuchElementException();
        }
        int position = head;
        byte[] bytes = new byte[slotSize];
        for (int i = 0; i < slotSize; i++) {
            bytes[i] = BUFFER.get(i + position);
        }
        size --;
        writeLong(size, SIZE_POS, SIZE_SIZ);
        return elementFactory.fromBytes(bytes);
    }

    @Override
    public T peek() throws IOException, NoSuchElementException, NullPointerException {
        if (size == 0) {
            throw new NoSuchElementException();
        }
        int position = head;
        byte[] bytes = new byte[slotSize];
        for (int i = 0; i < slotSize; i++) {
            bytes[i] = BUFFER.get(i + position);
        }
        return elementFactory.fromBytes(bytes);
    }
}
