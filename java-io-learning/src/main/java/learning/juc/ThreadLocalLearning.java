package learning.juc;

import java.util.concurrent.ThreadPoolExecutor;

public class ThreadLocalLearning {

  private static ThreadLocal<Integer> seqCount = new ThreadLocal<Integer>(){
    // 实现initialValue()
    public Integer initialValue() {
      return 0;
    }
  };

  public int nextSeq(){
    seqCount.set(seqCount.get() + 1);

    return seqCount.get();
  }

  public static void main(String[] args){
    ThreadLocalLearning seqCount = new ThreadLocalLearning();

    SeqThread thread1 = new SeqThread(seqCount);
    SeqThread thread2 = new SeqThread(seqCount);
    SeqThread thread3 = new SeqThread(seqCount);
    SeqThread thread4 = new SeqThread(seqCount);

    thread1.start();
    thread2.start();
    thread3.start();
    thread4.start();

    ThreadPoolExecutor threadPoolExecutor;
  }

  private static class SeqThread extends Thread{
    private ThreadLocalLearning seqCount;

    SeqThread(ThreadLocalLearning seqCount){
      this.seqCount = seqCount;
    }

    public void run() {
      for(int i = 0 ; i < 3 ; i++){
        System.out.println(Thread.currentThread().getName() + " seqCount :" + seqCount.nextSeq());
      }
    }
  }

}
