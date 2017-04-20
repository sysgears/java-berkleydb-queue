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
