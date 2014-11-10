package com.ashwinikd.mmqueue.impl;

import com.ashwinikd.mmqueue.MemoryMappedElement;
import com.ashwinikd.mmqueue.MemoryMappedElementFactory;
import com.ashwinikd.mmqueue.MemoryMappedQueue;
import com.ashwinikd.mmqueue.file.MemoryMappedFileException;
import com.ashwinikd.mmqueue.file.MemoryMappedQueueFile;

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
    private AtomicInteger dequeueSequence;
    private int head;
    private int tail;
    private int size;
    private volatile int cursor;
    private final int initialHead;
    private final int initialTail;
    private final int initialSize;
    private final int capacity;
    private final int slotSize;
    private final MappedByteBuffer BUFFER;
    private final MemoryMappedElementFactory<T> elementFactory;
    private AtomicInteger busyIterations;

    private static final int HEAD_POS = 0;
    private static final int HEAD_SIZ = 8;
    private static final int TAIL_POS = 8;
    private static final int TAIL_SIZ = 8;
    private static final int SIZE_POS = 16;
    private static final int SIZE_SIZ = 8;

    public MemoryMappedQueueImpl(MemoryMappedQueueFile file, MemoryMappedElementFactory<T> factory) throws IOException, MemoryMappedFileException {
        sequence = new AtomicInteger(0);
        BUFFER = file.getDataBuffer();
        int t = readInt(TAIL_POS, TAIL_SIZ);
        if(t == 0) {
            t = 32;
            writeLong(t, TAIL_POS, TAIL_SIZ);
        }
        int h = readInt(HEAD_POS, HEAD_SIZ);
        if (h == 0) {
            h = 32;
            writeLong(h, HEAD_POS, HEAD_SIZ);
        }
        initialHead = h;
        initialTail = t;
        head = initialHead;
        tail = initialTail;
        cursor = sequence.get();
        size = readInt(SIZE_POS, SIZE_SIZ);
        initialSize = size;
        capacity = file.getCapacity();
        slotSize = file.getSlotSize();
        elementFactory = factory;
        busyIterations = new AtomicInteger();
        dequeueSequence = new AtomicInteger();
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
        buffer.rewind();
        for (int i = 0; i < buffer.limit(); i++) {
            BUFFER.put(pos + i, buffer.get());
        }
    }

    private int readInt(int pos, int siz) {
        ByteBuffer buffer = ByteBuffer.allocate(siz);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < siz; i++) {
            buffer.put(BUFFER.get(pos + i));
        }
        buffer.rewind();
        return buffer.getInt();
    }

    private void writeInt(int n, int pos, int siz) {
        ByteBuffer buffer = ByteBuffer.allocate(siz);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(n);
        for (int i = 0; i < buffer.limit(); i++) {
            BUFFER.put(pos + i, buffer.get());
        }
    }

    public int getBusyIterations() {
        return busyIterations.get();
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
        if ((size + claimedSequence - cursor) > capacity) {
            sequence.decrementAndGet();
            return false;
        }
        int position = 32 + ((initialTail + slotSize * (claimedSequence - 1) - 32) % (capacity*slotSize));
        byte[] bytes = e.getBytes();
        for (int i = 0; i < bytes.length; i++) {
            BUFFER.put(i + position, bytes[i]);
        }
        int expectedSequence = claimedSequence - 1;
        int i = 0;
        while(cursor != expectedSequence) {
        }
        tail = 32 + ((position + slotSize - 32) % (capacity * slotSize));
        writeLong(position + slotSize, TAIL_POS, TAIL_SIZ);
        synchronized (this) {
            size++;
            writeLong(size, SIZE_POS, SIZE_SIZ);
        }
        cursor = claimedSequence;
        return true;
    }

    @Override
    public T dequeue() throws IOException, NoSuchElementException {
        int claimedSequence = dequeueSequence.incrementAndGet();
        if ((cursor - claimedSequence + initialSize) < 0) {
            dequeueSequence.decrementAndGet();
            throw new NoSuchElementException();
        }
        int position = 32 + ((initialHead - 32 + (claimedSequence - 1) * slotSize) % (capacity * slotSize));
        byte[] bytes = new byte[slotSize];
        for (int i = 0; i < slotSize; i++) {
            bytes[i] = BUFFER.get(i + position);
        }
        head = 32 + ((head + slotSize - 32) % (capacity * slotSize));
        writeLong(head, HEAD_POS, HEAD_SIZ);
        synchronized (this) {
            size --;
            writeLong(size, SIZE_POS, SIZE_SIZ);
        }
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

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[ ").append("MemoryMappedQueue").append("]\n\t").
        append("initialHead=").append(initialHead).append("\n\t").
        append("initialTail=").append(initialTail).append("\n\t").
        append("sequence=").append(sequence.get()).append("\n\t").
        append("head=").append(head).append("\n\t").
        append("tail=").append(tail).append("\n\t").
        append("size=").append(size).append("\n\t").
        append("capacity=").append(capacity).append("\n\t").
        append("slotSize=").append(slotSize);
        return sb.toString();
    }
}
