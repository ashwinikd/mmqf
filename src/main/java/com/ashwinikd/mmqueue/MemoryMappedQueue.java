package com.ashwinikd.mmqueue;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Memory mapped queue.
 *
 * @author Ashwini Dhekane[ashwini@ashwinidhekane.com]
 *
 * @see MemoryMappedElement
 * @see com.ashwinikd.mmqueue.file.MemoryMappedQueueFile
 *
 * @since 1.0
 */
public interface MemoryMappedQueue<T extends MemoryMappedElement> {


    /**
     * Get the number of elements in the queue.
     * @return size of the queue
     *
     * @since 1.0
     */
    public long size();

    /**
     * Checks if the queue is empty.
     * @return true if queue is empty, false otherwise
     *
     * @since 1.0
     */
    public boolean isEmpty();

    /**
     * Checks if the queue is full.
     * @return true if queue is full, false otherwise
     *
     * @since 1.0
     */
    public boolean isFull();

    /**
     * Get the capacity of the queue.
     *
     * @return capacity of the queue.
     */
    public long getCapacity();

    /**
     * Removes all elements from the queue.
     *
     * @throws IOException
     */
    public void clear() throws IOException;

    /**
     * Adds an element to the tail of the queue.
     * @param e element to be added
     * @return true if element was added, false otherwise
     * @throws java.lang.NullPointerException
     * @throws java.io.IOException
     * @since 1.0
     */
    public boolean enqueue(T e) throws IOException;

    /**
     * Remove an element from the head of the queue and
     * return it. If queue is empty this will throw
     * {@link java.util.NoSuchElementException}.
     *
     * @return the element
     * @throws java.lang.NullPointerException
     * @throws java.io.IOException
     * @throws java.util.NoSuchElementException
     * @since 1.0
     */
    public T dequeue() throws IOException, NoSuchElementException;

    /**
     * Return an element from the head of the queue but do not remove
     * it. If queue is empty then this will throw
     * {@link java.util.NoSuchElementException}
     *
     * @return the element on head
     * @throws IOException
     * @throws NoSuchElementException
     * @throws NullPointerException
     * @since 1.0
     */
    public T peek() throws IOException, NoSuchElementException, NullPointerException;
}
