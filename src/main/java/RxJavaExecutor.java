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

    /**
     * Schedules a callable to be run using an RxJava {@link io.reactivex.Flowable#timer(long, TimeUnit, Scheduler)}
     * <p>This is run on this RxJavaExecutor's {@link io.reactivex.internal.schedulers.ComputationScheduler}</p>
     * <p> Scheduling a callable with this is limiting as you can't react to the state of the callable return. Recommended to use a {@link io.reactivex.Flowable}
     * and one of this RxJavaExecutor's schedulers directly.</p>
     *
     * @param delay    time until the callable is called.
     * @param callable the callable to call
     * @return an Integer identifying the disposable returned by the Flowable. Can be used to cancel the scheduled callable.
     */
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

    /**
     * Schedules a runnable to be run using an RxJava {@link io.reactivex.Flowable#timer(long, TimeUnit, Scheduler)}
     * <p>This is run on this RxJavaExecutor's {@link io.reactivex.internal.schedulers.ComputationScheduler}</p>
     *
     * @param delay    time until the callable is called.
     * @param runnable the runnable to be run.
     * @return an Integer identifying the disposable returned by the Flowable. Can be used to cancel the scheduled runnable.
     */
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

    /**
     * Schedules a runnable to be run at a fix rate using an RxJava {@link io.reactivex.Flowable#interval(long, long, TimeUnit, Scheduler)}
     * <p>This is run on this RxJavaExecutor's {@link io.reactivex.internal.schedulers.ComputationScheduler}</p>
     *
     * @param delay    time in milliseconds until the runnable should start
     * @param period   the time in milliseconds between the runnable executing
     * @param runnable the runnable to be executed
     * @return an Integer identifying the disposable returned by the Flowable. Can be used to cancel the scheduled runnable.
     */
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

    /**
     * Schedules a callable to be run at a fixed rate using an RxJava {@link io.reactivex.Flowable#interval(long, long, TimeUnit, Scheduler)}
     * <p>This is run on this RxJavaExecutor's {@link io.reactivex.internal.schedulers.ComputationScheduler}</p>
     * <p> Scheduling a callable with this is limiting as you can't react to the state of each callable return. Recommended to use a {@link io.reactivex.Flowable}
     * and one of this RxJavaExecutor's schedulers directly.</p>
     *
     * @param delay    time in milliseconds until the runnable should start
     * @param period   the time in milliseconds between the runnable executing
     * @param callable the runnable to be executed
     * @return an Integer identifying the disposable returned by the Flowable. Can be used to cancel the scheduled callable.
     */
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

    /**
     * @return Scheduler to be used for single or scheduled tasks
     */
    public SingleScheduler getSingleScheduler() {
        return singleScheduler;
    }

    /**
     * @return Scheduler to be used for running computation heavy operations
     */
    public ComputationScheduler getComputationScheduler() {
        return computationScheduler;
    }

    /**
     * @return Scheduler to be used for running blocking operations
     */
    public IoScheduler getIoScheduler() {
        return ioScheduler;
    }

    /**
     * @param id returned by the disposable that should be cancelled.
     * @return true if disposable was cancelled otherwise false.
     */
    public synchronized boolean cancelScheduledDisposable(Integer id) {
        Disposable dis = removableDisposableMap.remove(id);

        if (dis != null) {
            dis.dispose();
            idGenerator.recycleId(id);
            return dis.isDisposed();
        }
        return false;
    }

    /**
     * Shuts down all RxJava Schedulers and the main Java Executor.
     * If no other threads are running program will exit.
     */
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
        Integer id = tester.scheduleFixedRateRunnable(0, 1000, () -> tester.printWhereRan("Computation runnable"));
        Integer id2 = tester.scheduleFixedRateCallable(0, 1000, () -> {
            tester.printWhereRan("Computation callable");
            return 0;
        });
        tester.scheduleSingleCallable(3000, () -> tester.cancelScheduledDisposable(id));
        tester.scheduleSingleCallable(3000, () -> tester.cancelScheduledDisposable(id2));
        tester.scheduleSingleRunnable(5000, () -> tester.shutdownExecutor());
    }

}
