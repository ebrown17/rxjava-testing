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
		Disposable disposable = Flowable.timer(delay, TimeUnit.MILLISECONDS, computationScheduler)
				.map(m -> callable.call()).observeOn(schedulerFromExecutor)
				.subscribe(onNext -> logger.info("scheduleSingleCallable subscriber thread {} ", onNext),
						error -> logger.error("scheduleSingleCallable error {} ", error.getMessage()), () -> {
							logger.info("scheduleSingleCallable Completed");
						});

		Integer id = idGenerator.getNewId();
		removableDisposableMap.put(id, disposable);

		return id;

	}

	public Integer scheduleSingleRunnable(long delay, Runnable runnable) {
		Disposable disposable = Flowable.timer(delay, TimeUnit.MILLISECONDS, computationScheduler)
				.doOnNext(m -> runnable.run()).observeOn(schedulerFromExecutor)
				.subscribe(onNext -> logger.info("scheduleSingleRunnable subscriber thread {} ", onNext),
						error -> logger.error("scheduleSingleRunnable error {} ", error.getMessage()),
						() -> {
							logger.info("scheduleSingleRunnable Completed");

						});

		  Integer id = idGenerator.getNewId();
	        removableDisposableMap.put(id, disposable);

	        return id;
	}

	public Integer scheduleFixedRateRunnable(long delay, long period, Runnable runnable) {
		Disposable disposable = Flowable.interval(delay, period, TimeUnit.MILLISECONDS, computationScheduler)
				.doOnNext(m -> runnable.run()).observeOn(schedulerFromExecutor).subscribe(i -> logger.info("{}", i));

		Integer id = idGenerator.getNewId();
        removableDisposableMap.put(id, disposable);

        return id;
	}

	public Integer scheduleFixedRateCallable(long delay, long period, Callable<? extends Object> callable) {
		Disposable disposable = Flowable.interval(delay, period, TimeUnit.MILLISECONDS, computationScheduler)
				.map(m -> callable.call()).observeOn(schedulerFromExecutor).subscribe(i -> logger.info("{}", i));

		Integer id = idGenerator.getNewId();
        removableDisposableMap.put(id, disposable);

        return id;
	}

	public synchronized boolean cancelScheduledDisposable(Integer id) {
		Disposable dis = removableDisposableMap.remove(id);

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

	public void testWherePrinted() {
		logger.info(" < Where I am running from");
	}

	public Callable<? extends Object> getCallableForTest() {
		Callable<String> c1 = () -> {
			testWherePrinted();
			return "Done";
		};
		return c1;

	}

	public void testScheduledCallable() {
		Callable<String> c1 = () -> {
			logger.info("callable 1 sleeping");
			Thread.sleep(2000);

			return "callable 1 done sleep for 2 seconds";
		};
		Integer disposable1 = scheduleFixedRateCallable(0, 3000, c1);
		Callable<String> c2 = () -> {
			logger.info("callable 2 sleeping");
			Thread.sleep(2000);

			return "callable 2 done sleep for 2 seconds";
		};
		Integer disposable2 = scheduleFixedRateCallable(1000, 3000, c2);

		Callable<Boolean> c3 = () -> {
			logger.info("callable 3 stopping intervals");
			logger.info("callable 3 disposable 1 id : {} id 2:{}", disposable1.hashCode(), disposable2.hashCode());
			cancelScheduledDisposable(disposable1);
			cancelScheduledDisposable(disposable2);

			return true;
		};
		scheduleSingleCallable(10000, c3);
	}

	public void testScheduledRunnable() {

		scheduleSingleRunnable(1000, () -> testWherePrinted());

		scheduleFixedRateRunnable(0, 1000, () -> testWherePrinted());
		scheduleFixedRateRunnable(0, 3000, () -> testWherePrinted());
		Integer id = scheduleSingleRunnable(12000, () -> shutdownExecutor());

		scheduleSingleRunnable(11000, () -> {
			logger.info("stopping shutdown disposable");
			cancelScheduledDisposable(id);
		});
	}

	public static void main(String[] args) {

		RxJavaExecutor tester = new RxJavaExecutor("main", 1);

		/*	tester.scheduleSingle(1000, "tester");
			
			tester.scheduleSingle(2000, "tester");
			tester.scheduleSingle(3000, "tester");
			tester.scheduleSingle(4000, "tester");
			tester.scheduleSingle(5000, "tester");*/

		tester.testScheduledCallable();
		tester.testScheduledRunnable();

	}

}
