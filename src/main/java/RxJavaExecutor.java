import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.Flowable;
import io.reactivex.Scheduler;
import io.reactivex.disposables.Disposable;
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
  private IdGenerator idGenerator;

  private Scheduler schedulerFromExecutor;

  private ConcurrentHashMap<Integer, Disposable> removableDisposableMap = new ConcurrentHashMap<Integer, Disposable>();

  public RxJavaExecutor(String name, int numberOfThreads) {
    logger = LoggerFactory.getLogger(name + "-RxJavaExecutor");
    idGenerator = new IdGenerator(name);
    namedThreadFactory = new NamedThreadFactory(name);
    namedExecutor = new ThreadPoolExecutor(numberOfThreads, numberOfThreads, 0L, TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<Runnable>(), namedThreadFactory);
    schedulerFromExecutor = Schedulers.from(namedExecutor);

    computationScheduler = new ComputationScheduler(new RxThreadFactory(name + "-RxComputationThreadPool"));
    ioScheduler = new IoScheduler(new RxThreadFactory(name + "-RxCachedThreadScheduler"));
    singleScheduler = new SingleScheduler(new RxThreadFactory(name + "-RxSingleScheduler"));

  }

  public Integer scheduleSingleCallable(long delay, Callable<? extends Object> callable) {
    Integer id = idGenerator.getNewId();
    Disposable disposable = Flowable.timer(delay, TimeUnit.MILLISECONDS, computationScheduler).map(m -> callable.call())
        .observeOn(schedulerFromExecutor)
        .subscribe(onNext -> logger.trace("scheduleSingleCallable {} ", onNext), error -> {
          logger.error("scheduleSingleCallable error {} ", error.getMessage());
          idGenerator.recycleId(id);
        }, () -> {
          logger.debug("scheduleSingleCallable Completed");
          idGenerator.recycleId(id);
        });

    removableDisposableMap.put(id, disposable);

    return id;
  }

  public Integer scheduleSingleRunnable(long delay, Runnable runnable) {
    Integer id = idGenerator.getNewId();
    Disposable disposable = Flowable.timer(delay, TimeUnit.MILLISECONDS, computationScheduler).doOnNext(m -> runnable.run())
        .observeOn(schedulerFromExecutor)
        .subscribe(onNext -> logger.trace("scheduleSingleRunnable {} ", onNext), error -> {
          logger.error("scheduleSingleRunnable error {} ", error.getMessage());
          idGenerator.recycleId(id);
        }, () -> {
          logger.debug("scheduleSingleRunnable Completed");
          idGenerator.recycleId(id);
        });

    removableDisposableMap.put(id, disposable);

    return id;
  }

  public Integer scheduleFixedRateRunnable(long delay, long period, Runnable runnable) {
    Integer id = idGenerator.getNewId();
    Disposable disposable =
        Flowable.interval(delay, period, TimeUnit.MILLISECONDS, computationScheduler).doOnNext(m -> runnable.run())
            .observeOn(schedulerFromExecutor).subscribe(i -> logger.trace("scheduleFixedRateRunnable id: {}", id), error -> {
              logger.error("scheduleFixedRateRunnable error {}", error.getMessage());
              idGenerator.recycleId(id);
            }, () -> {
              logger.debug("scheduleFixedRateRunnable Completed");
              idGenerator.recycleId(id);
            });

    removableDisposableMap.put(id, disposable);

    return id;
  }

  public Integer scheduleFixedRateCallable(long delay, long period, Callable<? extends Object> callable) {
    Integer id = idGenerator.getNewId();
    Disposable disposable =
        Flowable.interval(delay, period, TimeUnit.MILLISECONDS, computationScheduler).map(m -> callable.call())
            .observeOn(schedulerFromExecutor).subscribe(i -> logger.trace("scheduleFixedRateCallable {}", i), error -> {
              logger.error("scheduleFixedRateCallable error {}", error.getMessage());
              idGenerator.recycleId(id);
            }, () -> {
              logger.debug("scheduleFixedRateCallable Completed");
              idGenerator.recycleId(id);
            });

    removableDisposableMap.put(id, disposable);

    return id;
  }
  
  public SingleScheduler getSingleScheduler() {
    return singleScheduler;
  }

  public ComputationScheduler getComputationScheduler() {
    return computationScheduler;
  }

  public IoScheduler getIoScheduler() {
    return ioScheduler;
  }

  public synchronized boolean cancelScheduledDisposable(Integer id) {
    Disposable dis = removableDisposableMap.remove(id);
    idGenerator.recycleId(id);

    if (dis != null) {
      dis.dispose();
      return dis.isDisposed();
    }
    return false;
  }

  public void shutdownExecutor() {
    logger.info("Shutdown called, stopping all related schedulers and executor");
    schedulerFromExecutor.shutdown();
    singleScheduler.shutdown();
    computationScheduler.shutdown();
    ioScheduler.shutdown();
    namedExecutor.shutdown();
  }

  private void printWhereRan(String where) {
    logger.info("< {}", where);
  }
    
  public static void main(String[] args) {
    RxJavaExecutor tester = new RxJavaExecutor("main", 1);
    tester.scheduleSingleRunnable(0, () -> tester.printWhereRan("Computation"));
  }

}
