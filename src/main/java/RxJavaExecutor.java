import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.Flowable;
import io.reactivex.Scheduler;
import io.reactivex.internal.schedulers.ComputationScheduler;
import io.reactivex.internal.schedulers.IoScheduler;
import io.reactivex.internal.schedulers.RxThreadFactory;
import io.reactivex.internal.schedulers.SingleScheduler;
import io.reactivex.schedulers.Schedulers;

public class RxJavaExecutor {
  private Logger logger;
  private ThreadFactory namedThreadFactory;
  private ExecutorService namedExecutor;
  private SingleScheduler singleScheduler;
  private ComputationScheduler computationScheduler;
  private IoScheduler ioScheduler;

  private Scheduler schedulerFromExecutor;

  public RxJavaExecutor(String name, int numberOfThreads) {
    logger = LoggerFactory.getLogger(name + "-RxJavaExecutor");
    namedThreadFactory = new NamedThreadFactory(name);
    namedExecutor = new ThreadPoolExecutor(numberOfThreads, numberOfThreads, 0L, TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<Runnable>(), namedThreadFactory);
    schedulerFromExecutor = Schedulers.from(namedExecutor);

    computationScheduler = new ComputationScheduler(new RxThreadFactory(name + "-RxComputationThreadPool"));
    ioScheduler = new IoScheduler(new RxThreadFactory(name + "-RxCachedThreadScheduler"));
    singleScheduler = new SingleScheduler(new RxThreadFactory(name + "-RxSingleScheduler"));

  }


  public void scheduleSingle(long delay, String item) {
    Flowable.timer(delay, TimeUnit.MILLISECONDS, singleScheduler).doOnNext(oN -> testFromCallable())
        .observeOn(schedulerFromExecutor).subscribe(i -> logger.info("subscriber thread {}",i));
  }
  
  public void scheduleCallable(long delay, Callable<? extends Object> callable) {
    Flowable.timer(delay, TimeUnit.MILLISECONDS, singleScheduler).map(m -> callable.call())
       
        .observeOn(schedulerFromExecutor)
        .subscribe(onNext -> logger.info("subscriber thread {} ",onNext),
            error ->logger.error("error {} ",error.getMessage()),
                () -> {
                  logger.info("Completed");
                 
                });
  }

  public void testFromCallable() {
    logger.info("testFromCallable");
  }

  public static void main(String[] args) {

    RxJavaExecutor tester = new RxJavaExecutor("main", 1);
    tester.scheduleSingle(1000, "tester");

    tester.scheduleSingle(2000, "tester");
    tester.scheduleSingle(3000, "tester");
    tester.scheduleSingle(4000, "tester");
    tester.scheduleSingle(5000, "tester");
    
    tester.scheduleCallable(2000, new Callable<String>() {
     Logger logger = LoggerFactory.getLogger("-RxJavaExecutor");
      @Override
      public String call() throws Exception {
        logger.info("From callable");
        return "Done";
      }
      
    });
  }

}
