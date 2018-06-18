import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.Flowable;
import io.reactivex.Scheduler;
import io.reactivex.internal.schedulers.ComputationScheduler;
import io.reactivex.internal.schedulers.IoScheduler;
import io.reactivex.internal.schedulers.RxThreadFactory;
import io.reactivex.internal.schedulers.SingleScheduler;
import io.reactivex.schedulers.Schedulers;

public class Main {

  private final static Logger logger = LoggerFactory.getLogger("Main");
  private SingleScheduler singleScheduler;
  private ComputationScheduler computationScheduler;
  private IoScheduler ioScheduler;
  private ExecutorService namedExecutor;
  private ThreadFactory namedThreadFactory;
  private final Scheduler thisScheduler;

  public Main(String name) {
    namedThreadFactory = new NamedThreadFactory(name);
    namedExecutor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(),
        namedThreadFactory);
    thisScheduler = Schedulers.from(namedExecutor);
    
    computationScheduler = new ComputationScheduler(new RxThreadFactory(name + "-RxComputationThreadPool"));
    ioScheduler = new IoScheduler(new RxThreadFactory(name + "-RxCachedThreadScheduler"));
    singleScheduler = new SingleScheduler(new RxThreadFactory(name + "-RxSingleScheduler"));

  }

  public void start() {


    /*   namedExecutor.execute(new Runnable() {
      public void run() {
        computationScheduler.start();
        ioScheduler.start();
        scheduler.start();
      }
    });*/

    tester();

  }

  public void tester() {
    Flowable.range(1, 100).doOnNext(t -> {
      Thread.sleep(3000);
      logger.info("{}", t);
    }).subscribeOn(ioScheduler).map(t -> t).observeOn(thisScheduler).subscribe(msg -> {
      logger.info("{}", msg);
    });
    
    Flowable.range(1, 100).doOnNext(t -> {
      Thread.sleep(500);
      logger.info("{}", t);
    }).subscribeOn(computationScheduler).map(t -> t).observeOn(thisScheduler).subscribe(msg -> {
      logger.info("{}", msg);
    });
    Flowable.range(1, 100).doOnNext(t -> {
      Thread.sleep(2000);
      logger.info("{}", t);
    }).subscribeOn(singleScheduler).map(t -> t).observeOn(thisScheduler).subscribe(msg -> {
      logger.info("{}", msg);
    });
  }



  public static void main(String... args) throws InterruptedException {

    Main tester = new Main("MAIN");
    tester.start();

    Thread.sleep(5000);

  }

  static class NamedThreadFactory implements ThreadFactory {
    private final static Logger logger = LoggerFactory.getLogger("NamedThreadFactory");
    private static final AtomicInteger poolNumber = new AtomicInteger(1);
    private final ThreadGroup group;
    private final AtomicInteger threadNumber = new AtomicInteger(1);
    private final String namePrefix;

    NamedThreadFactory(String name) throws NullPointerException {
      if (null == name) {
        logger.error("NamedThreadFactory recieved null string name ");
        throw new NullPointerException("NamedThreadFactory recieved null name");
      }
      SecurityManager s = System.getSecurityManager();
      group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
      namePrefix = name + "-pool-" + poolNumber.getAndIncrement() + "-thread-";
    }

    public Thread newThread(Runnable r) {
      Thread t = new Thread(group, r, namePrefix + threadNumber.getAndIncrement(), 0);
      if (t.isDaemon())
        t.setDaemon(false);
      if (t.getPriority() != Thread.NORM_PRIORITY)
        t.setPriority(Thread.NORM_PRIORITY);
      return t;
    }
  }

}
