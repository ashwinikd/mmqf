package com.ashwinikd.mmqueue;

import com.ashwinikd.mmqueue.file.MemoryMappedFileException;
import com.ashwinikd.mmqueue.file.MemoryMappedQueueFile;
import com.ashwinikd.mmqueue.impl.MemoryMappedQueueImpl;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectStreamClass;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by ashwini on 10/11/14.
 */

public class MemoryMappedIntegerTest {

    private static final int N = 10000;
    private static final int NUM_THREADS = 20;
    private static final int LOOP_COUNT = N / NUM_THREADS;
    private static MemoryMappedQueueImpl<MemoryMappedInteger> queue;

    public static void main(String[] args) {
        //testConcurrent();
        testSimple();
    }

    private static  void testSleep() {
        long start = System.currentTimeMillis();
        for (int i = 0; i < 100; i++) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        long end = System.currentTimeMillis();
        System.out.println("Time taken: " + (end - start) + " ms");
    }

    private static void testConcurrent() {
        try {
            long uid = ObjectStreamClass.lookup(MemoryMappedInteger.class).getSerialVersionUID();
            MemoryMappedQueueFile mmqf;
            try {
                mmqf = new MemoryMappedQueueFile("/tmp/concurrent-" + N + ".mmqf");
            } catch (FileNotFoundException e) {
                mmqf = new MemoryMappedQueueFile("/tmp/concurrent-" + N + ".mmqf", uid, N, 4, false);
            }
            System.out.println(mmqf);
            MemoryMappedIntegerFactory factory = new MemoryMappedIntegerFactory();
            queue = new MemoryMappedQueueImpl<MemoryMappedInteger>(mmqf, factory);
            ExecutorService executor = Executors.newFixedThreadPool(4);
            for (int i = 0; i < NUM_THREADS; i++) {
                executor.submit(new QueueingTask(i));
            }
            executor.shutdown();
            executor.awaitTermination(1, TimeUnit.DAYS);
            System.out.println(queue);
            System.out.println(queue.size());
            System.out.println(queue.getBusyIterations());
            long l = queue.size();
            for (int j = 0; j < l; j++) {
                queue.dequeue().get();
            }
        } catch (MemoryMappedFileException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static class QueueingTask implements Runnable {
        private int id = 0;

        public QueueingTask(int i) {
            id = i;
        }

        @Override
        public void run() {
            int n = id * N / NUM_THREADS;
            long start = System.currentTimeMillis();
            for (int i = 0; i < LOOP_COUNT; i++) {
                try {
                    queue.enqueue(new MemoryMappedInteger(i + n));
                } catch (IOException e) {
                    System.err.println("Thread@" + id + " " + e.getMessage());

                }
            }
            long end = System.currentTimeMillis();
            System.out.println("Thread@" + id + " took " + (end - start) + "ms to queue " + LOOP_COUNT + " elements");
        }
    }

    private static void testSimple() {
        try {
            long uid = ObjectStreamClass.lookup(MemoryMappedInteger.class).getSerialVersionUID();
            MemoryMappedQueueFile mmqf;
            try {
                mmqf = new MemoryMappedQueueFile("/tmp/integer-" + N + ".mmqf");
            } catch (FileNotFoundException e) {
                mmqf = new MemoryMappedQueueFile("/tmp/integer-" + N + ".mmqf", uid, N, 4, false);
            }
            System.out.println(mmqf);
            MemoryMappedIntegerFactory factory = new MemoryMappedIntegerFactory();
            MemoryMappedQueueImpl<MemoryMappedInteger> queue = new MemoryMappedQueueImpl<MemoryMappedInteger>(mmqf, factory);
            long start = System.currentTimeMillis();
            for (int i = 0; i < N; i++) {
                queue.enqueue(new MemoryMappedInteger(i));
                //System.out.println(queue.dequeue().get());
            }
            long end = System.currentTimeMillis();
            System.out.println(queue);
            System.out.println("Time taken to queue: " + (end - start) + "ms");
            start = System.currentTimeMillis();
            for (int i = 0; i < N; i++) {
                int n = queue.dequeue().get();
                //System.out.println(n);
            }
            end = System.currentTimeMillis();
            System.out.println(queue);
            System.out.println("Time taken to dequeue: " + (end - start) + "ms");
        } catch (MemoryMappedFileException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
