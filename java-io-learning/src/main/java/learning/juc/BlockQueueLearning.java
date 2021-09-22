package learning.juc;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class BlockQueueLearning {

    public static void main(String[] args) throws InterruptedException {
        BlockingQueue a = new ArrayBlockingQueue(10);

        a.add("1");
        a.offer("1");
        a.take();
        a.poll();
        String xx = "";
    }
}
