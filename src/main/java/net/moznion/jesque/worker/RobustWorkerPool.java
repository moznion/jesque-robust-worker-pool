package net.moznion.jesque.worker;

import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import net.greghaines.jesque.worker.ExceptionHandler;
import net.greghaines.jesque.worker.JobFactory;
import net.greghaines.jesque.worker.Worker;
import net.greghaines.jesque.worker.WorkerEvent;
import net.greghaines.jesque.worker.WorkerEventEmitter;
import net.greghaines.jesque.worker.WorkerListener;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.stream.IntStream;

/**
 * Creates a fixed number of identical Workers, each on a separate {@code Thread}.
 * <p>
 * RobustWorkerPool monitors status of pooling workers and if it detects died worker then reincarnate a new worker.
 */
@Slf4j
public class RobustWorkerPool implements Worker {
    private static final long NO_DELAY = 0;

    private final int numWorkers;
    private final Set<Worker> workerSet;
    private final Map<Worker, Thread> workerThreadMap;
    private final WorkerPoolEventEmitter workerPoolEventEmitter;
    private final Callable<? extends Worker> workerFactory;
    private final ThreadFactory threadFactory;
    private final long delayToStartPollingMillis;

    private boolean isStarted;
    private boolean isEnded;
    private boolean isCalledJoin;
    private long joinMillis;

    /**
     * Create a RobustWorkerPool with the given number of Workers and the default
     * {@code ThreadFactory} and the default delay to start polling (default: no delay).
     *
     * @param workerFactory a Callable that returns an implementation of Worker
     * @param numWorkers    the number of Workers to create
     */
    public RobustWorkerPool(final Callable<? extends Worker> workerFactory, final int numWorkers) {
        this(workerFactory, numWorkers, Executors.defaultThreadFactory(), NO_DELAY);
    }

    /**
     * Create a RobustWorkerPool with the given number of Workers and the given
     * {@code ThreadFactory} and the default delay to start polling (default: no delay).
     *
     * @param workerFactory a Callable that returns an implementation of Worker
     * @param numWorkers    the number of Workers to create
     * @param threadFactory the factory to create pre-configured Threads
     */
    public RobustWorkerPool(final Callable<? extends Worker> workerFactory, final int numWorkers,
                            final ThreadFactory threadFactory) {
        this(workerFactory, numWorkers, threadFactory, NO_DELAY);
    }

    /**
     * Create a RobustWorkerPool with the given number of Workers and the given
     * {@code ThreadFactory} and the given delay to start polling.
     *
     * @param workerFactory             a Callable that returns an implementation of Worker
     * @param numWorkers                the number of Workers to create
     * @param threadFactory             the factory to create pre-configured Threads
     * @param delayToStartPollingMillis the milliseconds that represents delay to start polling when a new worker is spawned
     */
    public RobustWorkerPool(final Callable<? extends Worker> workerFactory, final int numWorkers,
                            final ThreadFactory threadFactory, final long delayToStartPollingMillis) {
        this.numWorkers = numWorkers;
        this.workerFactory = workerFactory;
        this.threadFactory = threadFactory;

        isStarted = false;
        isEnded = false;
        isCalledJoin = false;
        joinMillis = -1;

        workerSet = new HashSet<>(numWorkers);
        workerThreadMap = new HashMap<>(numWorkers);

        for (int i = 0; i < numWorkers; i++) {
            try {
                final Worker worker = workerFactory.call();
                workerSet.add(worker);
                workerThreadMap.put(worker, threadFactory.newThread(worker));
            } catch (RuntimeException re) {
                throw re;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        workerPoolEventEmitter = new WorkerPoolEventEmitter(this);

        this.delayToStartPollingMillis = delayToStartPollingMillis;
    }

    /**
     * Shutdown this pool and wait millis time per thread or until all threads are finished if millis is 0.
     *
     * @param now    if true, an effort will be made to stop any jobs in progress
     * @param millis the time to wait in milliseconds for the threads to join; a timeout of 0 means to wait forever.
     * @throws InterruptedException if any thread has interrupted the current thread.
     *                              The interrupted status of the current thread is cleared when this exception is thrown.
     */
    public void endAndJoin(final boolean now, final long millis) throws InterruptedException {
        end(now);
        join(millis);
    }

    /**
     * Join to internal threads and wait millis time per thread or until all
     * threads are finished if millis is 0.
     *
     * @param millis the time to wait in milliseconds for the threads to join; a
     *               timeout of 0 means to wait forever.
     * @throws InterruptedException if any thread has interrupted the current thread. The
     *                              interrupted status of the current thread is cleared when this
     *                              exception is thrown.
     */
    @Override
    public void join(final long millis) throws InterruptedException {
        isCalledJoin = true;
        joinMillis = millis;
        for (final Thread thread : workerThreadMap.values()) {
            thread.join(millis);
        }
    }

    @Override
    public String getName() {
        final StringBuilder sb = new StringBuilder(128 * workerThreadMap.size());
        String prefix = "";
        for (final Worker worker : workerSet) {
            sb.append(prefix).append(worker.getName());
            prefix = " | ";
        }
        return sb.toString();
    }

    @Override
    public WorkerEventEmitter getWorkerEventEmitter() {
        return this.workerPoolEventEmitter;
    }

    @Override
    public void run() {
        isStarted = true;
        workerThreadMap.values().forEach(java.lang.Thread::start);
        Thread.yield();
    }

    @Override
    public void end(final boolean now) {
        isEnded = true;
        for (final Worker worker : workerSet) {
            worker.end(now);
        }
    }

    @Override
    public boolean isShutdown() {
        final Iterator<Worker> iter = workerSet.iterator();
        return !iter.hasNext() || iter.next().isShutdown();
    }

    @Override
    public boolean isPaused() {
        final Iterator<Worker> iter = workerSet.iterator();
        return !iter.hasNext() || iter.next().isPaused();
    }

    @Override
    public void togglePause(final boolean paused) {
        for (final Worker worker : workerSet) {
            worker.togglePause(paused);
        }
    }

    @Override
    public boolean isProcessingJob() {
        boolean processingJob = false;
        for (final Worker worker : workerSet) {
            processingJob = worker.isProcessingJob();
            if (processingJob) {
                break;
            }
        }
        return processingJob;
    }

    @Override
    public JobFactory getJobFactory() {
        final Iterator<Worker> iter = workerSet.iterator();
        if (!iter.hasNext()) {
            return null;
        }
        return iter.next().getJobFactory();
    }

    @Override
    public Collection<String> getQueues() {
        final Iterator<Worker> iter = workerSet.iterator();
        if (!iter.hasNext()) {
            return null;
        }
        return iter.next().getQueues();
    }

    @Override
    public void addQueue(final String queueName) {
        for (final Worker worker : workerSet) {
            worker.addQueue(queueName);
        }
    }

    @Override
    public void removeQueue(final String queueName, final boolean all) {
        for (final Worker worker : workerSet) {
            worker.removeQueue(queueName, all);
        }
    }

    @Override
    public void removeAllQueues() {
        workerSet.forEach(net.greghaines.jesque.worker.Worker::removeAllQueues);
    }

    @Override
    public void setQueues(final Collection<String> queues) {
        for (final Worker worker : workerSet) {
            worker.setQueues(queues);
        }
    }

    @Override
    public ExceptionHandler getExceptionHandler() {
        final Iterator<Worker> iter = workerSet.iterator();
        if (!iter.hasNext()) {
            return null;
        }
        return iter.next().getExceptionHandler();
    }

    @Override
    public void setExceptionHandler(final ExceptionHandler exceptionHandler) {
        for (final Worker worker : workerSet) {
            worker.setExceptionHandler(exceptionHandler);
        }
    }

    /**
     * Reincarnate as a new worker form died worker.
     *
     * @param diedWorker target to reincarnate
     */
    private synchronized void reincarnateWorker(Worker diedWorker) {
        // Remove died worker from pooling information.
        workerSet.remove(diedWorker);
        workerThreadMap.remove(diedWorker);

        final int numCurrentWorkers = workerSet.size();
        log.debug("Number of current workers: {}", numCurrentWorkers);

        // Adjust number of workers when missing workers existed.
        // If already `end()` has called, do not reincarnate (make) a worker.
        if (!isEnded && numCurrentWorkers < numWorkers) {
            final int missingWorkersNum = numWorkers - numCurrentWorkers;
            log.debug("Missing workers: {}", missingWorkersNum);
            IntStream.rangeClosed(1, missingWorkersNum)
                    .forEach(i -> spawnMissingWorker(diedWorker));
        }
    }

    /**
     * Spawn a new worker. Spawned worker's information is based on died worker.
     *
     * @param diedWorker base worker
     */
    private void spawnMissingWorker(Worker diedWorker) {
        final Collection<String> queues = diedWorker.getQueues();
        final ExceptionHandler exceptionHandler = diedWorker.getExceptionHandler();
        final Map<WorkerEvent, Set<WorkerListener>> eventToListenersMap =
                workerPoolEventEmitter.getEventToListenersMapContainer().getEventToListenersMap();

        try {
            final Worker worker = workerFactory.call();
            worker.setQueues(queues); // restore queues.
            worker.setExceptionHandler(exceptionHandler); // restore exception handler.

            // restore event listeners.
            for (Map.Entry<WorkerEvent, Set<WorkerListener>> entry : eventToListenersMap.entrySet()) {
                WorkerEvent event = entry.getKey();
                Set<WorkerListener> listeners = entry.getValue();
                for (WorkerListener listener : listeners) {
                    worker.getWorkerEventEmitter().addListener(listener, event);
                }
            }

            // Add a new worker into pooling information.
            Thread newThread = threadFactory.newThread(worker);
            workerSet.add(worker);
            workerThreadMap.put(worker, newThread);

            if (isStarted) {
                newThread.start();
            }

            if (isCalledJoin) {
                newThread.join(joinMillis);
            }

            log.debug("Spawned a new worker (Worker Name: {})", worker.getName());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private class WorkerPoolEventEmitter implements WorkerEventEmitter {
        private final RobustWorkerPool pool;

        @Getter
        private final EventToListenerMapContainer eventToListenersMapContainer;

        public WorkerPoolEventEmitter(@NonNull RobustWorkerPool pool) {
            this.pool = pool;
            if (this.pool.workerSet == null) {
                throw new RuntimeException("Workers must not be null");
            }

            eventToListenersMapContainer = new EventToListenerMapContainer();

            // IF WORKER_START event is received, show message
            eventToListenersMapContainer.addListener(Collections.singletonList(WorkerEvent.WORKER_START),
                    (event, worker, queue, job, runner, result, t) -> {
                        log.debug("Worker is started (Worker Name: {})", worker.getName());
                        final long delay = this.pool.delayToStartPollingMillis;
                        if (delay > 0) {
                            try {
                                Thread.sleep(delay);
                            } catch (InterruptedException e) {
                                log.warn(e.toString());
                            }
                        }
                    });

            // If WORKER_ERROR event is received, kills such worker
            eventToListenersMapContainer.addListener(Collections.singletonList(WorkerEvent.WORKER_ERROR),
                    (event, worker, queue, job, runner, result, t) -> {
                        log.debug("Worker raise error (Worker Name: {})", worker.getName());
                        worker.end(false);
                    });

            // If WORKER_STOP event is received, reincarnate as a new worker
            eventToListenersMapContainer.addListener(Collections.singletonList(WorkerEvent.WORKER_STOP),
                    (event, worker, queue, job, runner, result, t) -> {
                        log.debug("Worker is stopped (Worker Name: {})", worker.getName());
                        reincarnateWorker(worker);
                    });

            // Register listeners that are at the above into each available workers.
            for (Map.Entry<WorkerEvent, Set<WorkerListener>> entry :
                    eventToListenersMapContainer.getEventToListenersMap().entrySet()) {
                final WorkerEvent event = entry.getKey();
                final Set<WorkerListener> listeners = entry.getValue();

                for (WorkerListener listener : listeners) {
                    addListener(listener, event);
                }
            }
        }

        @Override
        public void addListener(final WorkerListener listener) {
            for (final Worker worker : pool.workerSet) {
                worker.getWorkerEventEmitter().addListener(listener);
            }

            eventToListenersMapContainer.addListener(Arrays.asList(WorkerEvent.values()), listener);
        }

        @Override
        public void addListener(final WorkerListener listener, final WorkerEvent... events) {
            for (final Worker worker : pool.workerSet) {
                worker.getWorkerEventEmitter().addListener(listener, events);
            }

            eventToListenersMapContainer.addListener(Arrays.asList(events), listener);
        }

        @Override
        public void removeListener(final WorkerListener listener) {
            for (final Worker worker : pool.workerSet) {
                worker.getWorkerEventEmitter().removeListener(listener);
            }

            eventToListenersMapContainer.removeListener(Arrays.asList(WorkerEvent.values()), listener);
        }

        @Override
        public void removeListener(final WorkerListener listener, final WorkerEvent... events) {
            for (final Worker worker : pool.workerSet) {
                worker.getWorkerEventEmitter().removeListener(listener, events);
            }

            eventToListenersMapContainer.removeListener(Arrays.asList(events), listener);
        }

        @Override
        public void removeAllListeners() {
            for (final Worker worker : pool.workerSet) {
                worker.getWorkerEventEmitter().removeAllListeners();
            }

            eventToListenersMapContainer.removeAllListeners(Arrays.asList(WorkerEvent.values()));
        }

        @Override
        public void removeAllListeners(final WorkerEvent... events) {
            for (final Worker worker : pool.workerSet) {
                worker.getWorkerEventEmitter().removeAllListeners(events);
            }

            eventToListenersMapContainer.removeAllListeners(Arrays.asList(events));
        }
    }

    /**
     * Container of Map that has relationship between WorkerEvent and set of WorkerListeners.
     * <p>
     * This class used to restore listeners when reincarnating a new worker.
     */
    private static class EventToListenerMapContainer {
        @Getter
        private final Map<WorkerEvent, Set<WorkerListener>> eventToListenersMap;

        public EventToListenerMapContainer() {
            eventToListenersMap = new HashMap<>();

            // initialize for each events.
            for (WorkerEvent event : WorkerEvent.values()) {
                eventToListenersMap.put(event, new HashSet<>());
            }
        }

        public void addListener(List<WorkerEvent> events, WorkerListener listener) {
            for (WorkerEvent event : events) {
                eventToListenersMap.get(event).add(listener);
            }
        }

        public void removeListener(List<WorkerEvent> events, WorkerListener listener) {
            for (WorkerEvent event : events) {
                eventToListenersMap.get(event).remove(listener);
            }
        }

        public void removeAllListeners(List<WorkerEvent> events) {
            for (WorkerEvent event : events) {
                eventToListenersMap.get(event).clear();
            }
        }
    }
}
