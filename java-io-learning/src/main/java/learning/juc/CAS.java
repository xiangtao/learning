package learning.juc;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class CAS {

    public static void main(String[] args) {
        AtomicInteger atomicInteger = new AtomicInteger();
        atomicInteger.incrementAndGet();

        //AtomicStampedReference atomicStampedReference = new AtomicStampedReference();
        ReentrantLock lock = new ReentrantLock();
        lock.lock();
        lock.tryLock();

        ConcurrentHashMap map = new ConcurrentHashMap();
        map.get("a");
        BlockingQueue queue = new ArrayBlockingQueue(1);
        /*ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 2, 1,
            TimeUnit.SECONDS, null);*/

        ThreadLocal local = new ThreadLocal();

        local.set("a");
        System.out.println(0x61c88647);
    }

}
