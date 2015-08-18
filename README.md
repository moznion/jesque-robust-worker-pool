jesque-robust-worker-pool [![Build Status](https://travis-ci.org/moznion/jesque-robust-worker-pool.svg)](https://travis-ci.org/moznion/jesque-robust-worker-pool)
=============

An implementation of worker pooling for [jesque](https://github.com/gresrun/jesque).
It monitors status of pooling workers and if it detects died worker then adjust number of workers.

Synopsis
---

```java
final Config config = new ConfigBuilder().build();
final Map<String, Class<? extends Runnable>> actionMap = new HashMap<>();
actionMap.put("TestAction", TestAction.class);

final RobustWorkerPool workerPool = new RobustWorkerPool(() ->
    new WorkerImpl(config, Collections.singletonList("foo"), new MapBasedJobFactory(actionMap)),
    10, Executors.defaultThreadFactory());

// Highly recommend: Handling to recover for `WORKER_ERROR` event should be set like so:
workerPool.getWorkerEventEmitter().addListener(
    (event, worker, queue, errorJob, runner, result, t) -> {
        log.info("Something handling to recover a worker when it fires `WORKER_ERROR` event");
        log.info("You have the option of implementing error handling.");
        // Do something
    }, WorkerEvent.WORKER_ERROR
)

workerPool.run();

// Uncomment followings with a situation.
// workerPool.end(false);
// workerPool.endAndJoin(false, 0);
// ...
```

Requires
--

- Java 8 or later
- Jesque 2.0.2 or later

Motivation
--

`WorkerPool` is an implementation of worker pooling which is provided by jesque core.
However that doesn't have any interest in worker's status, alive or not.
Means worker will not revive even if any workers die on polling.

Such behavior is not robust if making workers work long hours.
So I make this implementation. This worker pool monitors about worker is alive or not.
It also adjust number of workers (if missing workers exist then make new workers, and vice versa)
automatically when a worker is stopped.

Features (different from original `WorkerPool`)
--

- Monitors status of pooled workers, dead or alive.
- Adjust number of active workers. When a pooled worker is died on polling, manager will create a new worker and join it into pool.

Tips
--

### How to add a listener for any event?

Use `robustWorkerPool.getWorkerEventEmitter().addListener()` method. Please also refer to the above synopsis.

See Also
--

- [jesque](https://github.com/gresrun/jesque)

Author
--

moznion (<moznion@gmail.com>)

License
--

```
Copyright 2011 Greg Haines
Copyright 2015 moznion, http://moznion.net/ <moznion@gmail.com>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```

