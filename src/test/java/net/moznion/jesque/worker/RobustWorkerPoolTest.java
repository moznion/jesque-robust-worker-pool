package net.moznion.jesque.worker;

import lombok.extern.slf4j.Slf4j;
import net.greghaines.jesque.Config;
import net.greghaines.jesque.ConfigBuilder;
import net.greghaines.jesque.Job;
import net.greghaines.jesque.client.Client;
import net.greghaines.jesque.client.ClientImpl;
import net.greghaines.jesque.worker.JobFactory;
import net.greghaines.jesque.worker.MapBasedJobFactory;
import net.greghaines.jesque.worker.WorkerImpl;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;

import static org.assertj.core.api.StrictAssertions.assertThat;

/**
 * Testing for {@link RobustWorkerPool}.
 * <p>
 * NOTES: redis-server must upped on testing environment.
 */
@Slf4j
public class RobustWorkerPoolTest {
    private static final Config CONFIG = new ConfigBuilder().build();

    @Before
    public void setup() {
        Jedis jedis = new Jedis("localhost");
        jedis.flushAll();
    }

    @Test
    public void shouldWorkerPoolingSuccessfully() throws InterruptedException {
        // Add a job to the queue
        final Job job = new Job("TestAction", Collections.emptyList());
        final Client client = new ClientImpl(CONFIG);
        client.enqueue("foo", job);
        client.end();

        // Start a worker to run jobs from the queue
        final Map<String, Class<? extends Runnable>> actionMap = new HashMap<>();
        actionMap.put("TestAction", TestAction.class);

        final RobustWorkerPool workerPool = new RobustWorkerPool(() ->
                new WorkerImpl(CONFIG, Collections.singletonList("foo"), new MapBasedJobFactory(actionMap)),
                10,
                (event, worker, queue, errorJob, runner, result, t) -> {
                    log.debug("Worker raise error (Worker Name: {})", worker.getName());
                    worker.end(false);
                },
                Executors.defaultThreadFactory());

        workerPool.run();

        // XXX: dirty...
        // To wait until all workers are spawned
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
        }

        assertThat(workerPool.getNumWorkers()).isEqualTo(10);
        assertThat(workerPool.getWorkerSet().size()).isEqualTo(10);
        assertThat(workerPool.getWorkerThreadMap().size()).isEqualTo(10);

        workerPool.endAndJoin(false, 0);
    }

    @Test
    public void shouldMissingWorkerReincarnationSuccessfully() throws InterruptedException {
        // Add a job to the queue
        final Job job = new Job("TestAction", Collections.emptyList());
        final Client client = new ClientImpl(CONFIG);
        client.enqueue("foo", job);
        client.end();

        // Start a worker to run jobs from the queue
        final Map<String, Class<? extends Runnable>> actionMap = new HashMap<>();
        actionMap.put("TestAction", TestAction.class);

        final RobustWorkerPool workerPool = new RobustWorkerPool(() ->
                new FailingWorker(CONFIG, Collections.singletonList("foo"), new MapBasedJobFactory(actionMap)),
                10,
                (event, worker, queue, errorJob, runner, result, t) -> {
                    log.debug("Worker raise error (Worker Name: {})", worker.getName());
                    worker.end(false);
                },
                Executors.defaultThreadFactory(), 500);

        workerPool.run();

        // XXX: dirty...
        // To wait until all workers are spawned
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
        }

        assertThat(workerPool.getNumWorkers()).isEqualTo(10);
        assertThat(workerPool.getWorkerSet().size()).isEqualTo(10);
        assertThat(workerPool.getWorkerThreadMap().size()).isEqualTo(10);
    }

    @Test
    public void shouldExcessWorkerTerminationSuccessfully() throws InterruptedException {
        // Add a job to the queue
        final Job job = new Job("TestAction", Collections.emptyList());
        final Client client = new ClientImpl(CONFIG);
        client.enqueue("foo", job);
        client.end();

        // Start a worker to run jobs from the queue
        final Map<String, Class<? extends Runnable>> actionMap = new HashMap<>();
        actionMap.put("TestAction", TestAction.class);

        final RobustWorkerPool workerPool = new RobustWorkerPool(() ->
                new FailingWorker(CONFIG, Collections.singletonList("foo"), new MapBasedJobFactory(actionMap)),
                10,
                (event, worker, queue, errorJob, runner, result, t) -> {
                    log.debug("Worker raise error (Worker Name: {})", worker.getName());
                    worker.end(false);
                },
                Executors.defaultThreadFactory(), 500);

        workerPool.setNumWorkers(8); // change number of upper limit of workers

        workerPool.run();

        // XXX: dirty...
        // To wait until all workers are spawned
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
        }

        assertThat(workerPool.getNumWorkers()).isEqualTo(8);
        assertThat(workerPool.getWorkerSet().size()).isEqualTo(8);
        assertThat(workerPool.getWorkerThreadMap().size()).isEqualTo(8);
    }

    public static class TestAction implements Runnable {
        public void run() {
            log.debug("Run");
        }
    }

    public static class FailingWorker extends WorkerImpl {
        public FailingWorker(Config config, Collection<String> queues, JobFactory jobFactory) {
            super(config, queues, jobFactory);
        }

        @Override
        public void poll() {
            log.debug("Fail");
            this.recoverFromException(null, new RuntimeException());
        }
    }
}
