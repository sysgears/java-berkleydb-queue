## Lightweight fast persistent queue in Java using Berkley DB

My original article has appeared here first:
https://sysgears.com/articles/lightweight-fast-persistent-queue-in-java-using-berkley-db/

I have uploaded the code for my article here to clarify that
it has Public Domain license.

## Article

Recently I had a task to develop the application which will have large 
work queue and which need to survive the restarts. The application need 
to be lightweight. After trying several different persistent engines for
Java I''ve chosen to stick with Berkley DB Java edition. This persistent
engine is pretty lightweight it is fast, optimized for multi-threaded 
usage and have no problems with reclaiming free space.

As I needed the fast persistent queue at a cost of possible data loss on
system crash I've chosen non-transactional API for Berkley DB. With 
non-transactional API the great speed can be achieved for persistent 
queue at a price of loss of some data at system crash. The more data you
allow to be lost the greater speed of the queue you will have. Though 
you can opt to sync to disk each operation on the queue and in that case
your data loss will be minimal.

<!--more-->

Berkley DB keeps data sorted by key in B-Tree. By default keys are 
sorted lexicographically byte by byte. But you can override sorting 
order by providing your own comparator. In this implementation of the 
queue keys are just big integers and sorted in ascending order.

The DB allows row locking model, this is great for doing multi-threaded 
polling of the queue. But for multi-threaded pushing you either need to 
keep key counter in separate database or synchronize the push method. 
I've decided to choose later route.

So, here is the code of the queue with described choices:
``` java

package com.sysgears;

import com.sleepycat.je.*;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigInteger;
import java.util.Comparator;

/**
 * Key comparator for DB keys
 */
class KeyComparator implements Comparator, Serializable {

    /**
     * Compares two DB keys.
     *
     * @param key1 first key
     * @param key2 second key
     *
     * @return comparison result
     */
    public int compare(byte[] key1, byte[] key2) {
        return new BigInteger(key1).compareTo(new BigInteger(key2));
    }
}

/**
 * Fast queue implementation on top of Berkley DB Java Edition.
 *

 * This class is thread-safe.
 */
public class Queue {

    /**
     * Berkley DB environment
     */
    private final Environment dbEnv;

    /**
     * Berkley DB instance for the queue
     */
    private final Database queueDatabase;

    /**
     * Queue cache size - number of element operations it is allowed to loose in case of system crash.
     */
    private final int cacheSize;

    /**
     * This queue name.
     */
    private final String queueName;

    /**
     * Queue operation counter, which is used to sync the queue database to disk periodically.
     */
    private int opsCounter;

    /**
     * Creates instance of persistent queue.
     *
     * @param queueEnvPath   queue database environment directory path
     * @param queueName      descriptive queue name
     * @param cacheSize      how often to sync the queue to disk
     */
    public Queue(final String queueEnvPath,
                 final String queueName,
                 final int cacheSize) {
        // Create parent dirs for queue environment directory
        new File(queueEnvPath).mkdirs();

        // Setup database environment
        final EnvironmentConfig dbEnvConfig = new EnvironmentConfig();
        dbEnvConfig.setTransactional(false);
        dbEnvConfig.setAllowCreate(true);
        this.dbEnv = new Environment(new File(queueEnvPath),
                                  dbEnvConfig);

        // Setup non-transactional deferred-write queue database
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(false);
        dbConfig.setAllowCreate(true);
        dbConfig.setDeferredWrite(true);
        dbConfig.setBtreeComparator(new KeyComparator());
        this.queueDatabase = dbEnv.openDatabase(null,
            queueName,
            dbConfig);
        this.queueName = queueName;
        this.cacheSize = cacheSize;
        this.opsCounter = 0;
    }

    /**
     * Retrieves and returns element from the head of this queue.
     *
     * @return element from the head of the queue or null if queue is empty
     *
     * @throws IOException in case of disk IO failure
     */
    public String poll() throws IOException {
        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry();
        final Cursor cursor = queueDatabase.openCursor(null, null);
        try {
            cursor.getFirst(key, data, LockMode.RMW);
            if (data.getData() == null)
                return null;
            final String res = new String(data.getData(), "UTF-8");
            cursor.delete();
            opsCounter++;
            if (opsCounter >= cacheSize) {
                queueDatabase.sync();
                opsCounter = 0;
            }
            return res;
        } finally {
            cursor.close();
        }
    }

    /**
     * Pushes element to the tail of this queue.
     *
     * @param element element
     *
     * @throws IOException in case of disk IO failure
     */
    public synchronized void push(final String element) throws IOException {
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        Cursor cursor = queueDatabase.openCursor(null, null);
        try {
            cursor.getLast(key, data, LockMode.RMW);

            BigInteger prevKeyValue;
            if (key.getData() == null) {
                prevKeyValue = BigInteger.valueOf(-1);
            } else {
                prevKeyValue = new BigInteger(key.getData());
            }
            BigInteger newKeyValue = prevKeyValue.add(BigInteger.ONE);

            try {
                final DatabaseEntry newKey = new DatabaseEntry(
                        newKeyValue.toByteArray());
                final DatabaseEntry newData = new DatabaseEntry(
                        element.getBytes("UTF-8"));
                queueDatabase.put(null, newKey, newData);

                opsCounter++;
                if (opsCounter >= cacheSize) {
                    queueDatabase.sync();
                    opsCounter = 0;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        } finally {
            cursor.close();
        }
    }

   /**
     * Returns the size of this queue.
     *
     * @return the size of the queue
     */
    public long size() {
        return queueDatabase.count();
    }

    /**
     * Returns this queue name.
     *
     * @return this queue name
     */
    public String getQueueName() {
        return queueName;
    }

    /**
     * Closes this queue and frees up all resources associated to it.
     */
    public void close() {
        queueDatabase.close();
        dbEnv.close();
    }
}
```

The unit tests for the queue:
``` java

package com.sysgears;

import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Test
public class UTestQueue {

    @Test
    public void testCreateQueue() {
        File queueDir = TestUtils.createTempSubdir("test-queue");
        Queue queue = new Queue(queueDir.getPath(), "test-queue", 3);
        try {
            assert Arrays.asList(queueDir.listFiles()).contains(new File(queueDir, "00000000.jdb"));
        } finally {
            queue.close();
        }
    }

    @Test public void testPush() throws Throwable {
        File queueDir = TestUtils.createTempSubdir("test-queue");
        Queue queue = new Queue(queueDir.getPath(), "test-queue", 3);
        try {
            queue.push("1");
            queue.push("2");
            String head = queue.poll();

            assert head.equals("1");
        } finally {
            queue.close();
        }
    }

    @Test public void testQueueSurviveReopen() throws Throwable {
        File queueDir = TestUtils.createTempSubdir("test-queue");
        Queue queue = new Queue(queueDir.getPath(), "test-queue", 3);
        try {
            queue.push("5");
        } finally {
            queue.close();
        }

        queue = new Queue(queueDir.getPath(), "test-queue", 3);
        try {
            String head = queue.poll();

            assert head.equals("5");
        } finally {
            queue.close();
        }
    }

    @Test public void testQueuePushOrder() throws Throwable {
        File queueDir = TestUtils.createTempSubdir("test-queue");
        final Queue queue = new Queue(queueDir.getPath(), "test-queue", 1000);
        try {
            for (int i = 0; i < 300; i++) {
                queue.push(Integer.toString(i));
            }

            for (int i = 0; i < 300; i++) {
                String element = queue.poll();
                if (!Integer.toString(i).equals(element)) {
                    throw new AssertionError("Expected element " + i + ", but got " + element);
                }
            }
        } finally {
            queue.close();
        }

    }

    @Test public void testMultiThreadedPoll() throws Throwable {
        File queueDir = TestUtils.createTempSubdir("test-queue");
        final Queue queue = new Queue(queueDir.getPath(), "test-queue", 3);
        try {
            int threadCount = 20;
            for (int i = 0; i < threadCount; i++)
                queue.push(Integer.toString(i));

            final Set set = Collections.synchronizedSet(new HashSet());
            final CountDownLatch startLatch = new CountDownLatch(threadCount);
            final CountDownLatch latch = new CountDownLatch(threadCount);

            for (int i = 0; i < threadCount; i++) {
                new Thread() {
                    public void run() {
                        try {
                            startLatch.countDown();
                            startLatch.await();

                            String val = queue.poll();
                            if (val != null) {
                                set.add(val);
                            }
                            latch.countDown();
                        } catch (Throwable e) {
                            e.printStackTrace();
                        }
                    }
                }.start();
            }

            latch.await(5, TimeUnit.SECONDS);

            assert set.size() == threadCount;
        } finally {
            queue.close();
        }
    }

    @Test public void testMultiThreadedPush() throws Throwable {
        File queueDir = TestUtils.createTempSubdir("test-queue");
        final Queue queue = new Queue(queueDir.getPath(), "test-queue", 3);
        try {
            int threadCount = 20;

            final CountDownLatch startLatch = new CountDownLatch(threadCount);
            final CountDownLatch latch = new CountDownLatch(threadCount);

            for (int i = 0; i < threadCount; i++) {
                new Thread(Integer.toString(i)) {
                    public void run() {
                        try {
                            startLatch.countDown();
                            startLatch.await();

                            queue.push(getName());
                            latch.countDown();
                        } catch (Throwable e) {
                            e.printStackTrace();
                        }
                    }
                }.start();
            }

            latch.await(5, TimeUnit.SECONDS);

            assert queue.size() == threadCount;
        } finally {
            queue.close();
        }
    }
}
```

And here is a main class which measures the queue performance:
``` java

package com.sysgears;

import java.io.File;

public class Main {

    public static void main(String[] args) throws Throwable {
        int elementCount = 10000;
        File queueDir = TestUtils.createTempSubdir("test-queue");
        final Queue queue = new Queue(queueDir.getPath(), "test-queue", 1000);
        try {
            long pushStart = System.currentTimeMillis();
            for (int i = 0; i < elementCount; i++) {
                queue.push(Integer.toString(i));
            }
            long pushEnd = System.currentTimeMillis();
            System.out.println("Time to push " + elementCount + " records: " + (pushEnd - pushStart) + " ms");

            long pollStart = System.currentTimeMillis();
            for (int i = 0; i < elementCount; i++) {
                String element = queue.poll();
                if (!Integer.toString(i).equals(element)) {
                    throw new AssertionError("Expected element " + i + ", but got " + element);
                }
            }
            long pollEnd = System.currentTimeMillis();
            System.out.println("Time to poll " + elementCount + " records: " + (pollEnd - pollStart) + " ms");
        } finally {
            queue.close();
        }
    }
}
```

Output of running main on my PC:
<pre>

Time to push 10000 records: 2633 ms
Time to poll 10000 records: 7764 ms</pre>

Hope this helps,  
Victor Vlasenko,
SysGears

## Stay In Touch
For the latest articles, follow on Twitter: [@sysgears](https://twitter.com/sysgears)
