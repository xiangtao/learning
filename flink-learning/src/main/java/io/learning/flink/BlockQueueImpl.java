package io.learning.flink;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class BlockQueueImpl<T> {

  private int size;
  private List list;
  private Object lock = new Object();

  public BlockQueueImpl(int size){
    this.size = size;
    list = new ArrayList(size);
  }

  public synchronized T poll(){
    if(list.size()<=0){
      try {
        this.wait();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    T t = (T) list.get(0);
    list.remove(0);
    this.notify();
    return t;
  }

  public synchronized void add(T t){
    if(list.size() == size){
      try {
        this.wait();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    list.add(t);
    this.notify();
  }

  public static void main(String[] args) {
    BlockQueueImpl<String> blockQueue = new BlockQueueImpl<String>(2);
    blockQueue.add("123");
    blockQueue.add("456");
    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          Thread.sleep(10 * 1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        String item = blockQueue.poll();
        System.out.println(item);
      }
    });
    thread.start();

    blockQueue.add("789");

    ReentrantLock reentrantLock;

  }


}
