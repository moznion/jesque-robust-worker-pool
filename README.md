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

workerPool.run();

# workerPool.endAndJoin(false, 0);
```

Requires
--

- Java 8 or later
- Jesque 2.0.2 or later

Motivation
--

`WorkerPool` is an implementation of worker pooling which is provided by jesque core.
However that doesn't any have interest in worker's status, alive or not.
Means worker will not revive even if any worker die by unfortunate accident.
Such behavior is not robust if making workers work long hours.
So I make this implementation. This worker pool monitors about worker is alive or not.
It also adjust number of workers (if missing workers exist then make new workers, and vice versa)
automatically when any worker is stopped.

Author
--

moznion (<moznion@gmail.com>)

License
--

```
The MIT License (MIT)
Copyright © 2015 moznion, http://moznion.net/ <moznion@gmail.com>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the “Software”), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
```

