# 🧵 Java Multithreading — Interview Notes

<p align="center">
  <b>Threads · Locks · Executors · CompletableFuture · Reactive (WebFlux) — Concepts & Traps</b>
</p>

---

## 📑 Table of Contents

1. [Process vs Thread](#1-process-vs-thread)
2. [Thread Lifecycle](#2-thread-lifecycle)
3. [Race Condition](#3-race-condition)
4. [synchronized](#4-synchronized)
5. [Monitor Lock](#5-monitor-lock)
6. [ReentrantLock](#6-reentrantlock)
7. [wait() / notify() / notifyAll()](#7-wait--notify--notifyall)
8. [sleep() vs wait()](#8-sleep-vs-wait)
9. [Deadlock](#9-deadlock)
10. [Livelock & Starvation](#10-livelock--starvation)
11. [volatile](#11-volatile)
12. [Atomic Classes & CAS](#12-atomic-classes--cas)
13. [Thread Pool](#13-thread-pool)
14. [ExecutorService](#14-executorservice)
15. [Types of Executors](#15-types-of-executors)
16. [Runnable vs Callable](#16-runnable-vs-callable)
17. [Future](#17-future)
18. [CompletableFuture](#18-completablefuture)
19. [Concurrency vs Parallelism](#19-concurrency-vs-parallelism)
20. [Blocking vs Non-blocking I/O](#20-blocking-vs-non-blocking-io)
21. [WebFlux + R2DBC](#21-webflux--r2dbc)
22. [Mono vs Flux](#22-mono-vs-flux)
23. [Reactive Pipeline & subscribe()](#23-reactive-pipeline--subscribe)
24. [map() vs flatMap()](#24-map-vs-flatmap)
25. [Backpressure](#25-backpressure)
26. [ConcurrentHashMap](#26-concurrenthashmap)
27. [CountDownLatch](#27-countdownlatch)
28. [CyclicBarrier](#28-cyclicbarrier)
29. [Semaphore](#29-semaphore)
30. [ReadWriteLock](#30-readwritelock)
31. [shutdown() vs shutdownNow()](#31-shutdown-vs-shutdownnow)
32. [ThreadLocal](#32-threadlocal)
33. [Fork/Join Framework](#33-forkjoin-framework)
34. [BlockingQueue](#34-blockingqueue)
35. [StampedLock](#35-stampedlock)
36. [Exchanger](#36-exchanger)
37. [happens-before Relationship](#37-happens-before-relationship)
38. [Double-Checked Locking](#38-double-checked-locking)
39. [Virtual Threads (Project Loom, Java 21+)](#39-virtual-threads-project-loom-java-21)
40. [Common Interview Traps](#40-common-interview-traps)
41. [⭐ Most Important Topics to Revise](#41--most-important-topics-to-revise)

---

## 1. Process vs Thread

**Process**
- Program in execution.
- Has its own memory space.
- Relatively heavyweight.

**Thread**
- Smallest unit of execution inside a process.
- Threads share the process's heap memory.
- Each thread has its own stack and program counter.
- Lightweight compared with processes.

```
Process
 ├── Thread 1 → Stack + PC
 ├── Thread 2 → Stack + PC
 └── Thread 3 → Stack + PC
Shared → Heap
```

---

## 2. Thread Lifecycle

A Java thread moves through these states (`Thread.State`):

```
NEW → RUNNABLE → (BLOCKED / WAITING / TIMED_WAITING) → TERMINATED
```

| State | Meaning |
|---|---|
| `NEW` | Thread object created, `start()` not yet called |
| `RUNNABLE` | Eligible to run (may be actually running or waiting for CPU) |
| `BLOCKED` | Waiting to acquire a monitor lock |
| `WAITING` | Waiting indefinitely (`wait()`, `join()`, `LockSupport.park()`) |
| `TIMED_WAITING` | Waiting with a timeout (`sleep()`, `wait(timeout)`, `join(timeout)`) |
| `TERMINATED` | Run completed or threw an exception |

> `start()` schedules a new thread of execution. Calling `run()` directly just executes the method on the current thread — no new thread is created.

---

## 3. Race Condition

A race condition occurs when multiple threads access shared data concurrently and the result depends on the timing of execution.

Example:
```java
count++;
```

This is actually three steps:
```
read count
   ↓
increment
   ↓
write count
```

Two threads can read the same old value and overwrite each other's update.

**Solutions**
- `synchronized`
- `ReentrantLock`
- `AtomicInteger`
- Concurrent collections
- Proper immutable design

---

## 4. synchronized

Provides mutual exclusion.

```java
synchronized void increment() {
    count++;
}
```

Only one thread can execute the synchronized section for the same monitor at a time.

**Synchronized block**
```java
synchronized (lock) {
    // critical section
}
```

- **Advantage:** simple, JVM-managed.
- **Disadvantage:** less control compared with `ReentrantLock`.

---

## 5. Monitor Lock

Every Java object can act as a monitor. When you do:

```java
synchronized (obj) {
    // code
}
```

the thread must acquire `obj`'s monitor lock.

```
Thread
   ↓
acquire monitor
   ↓
execute synchronized code
   ↓
release monitor
```

When the synchronized block/method finishes, the JVM releases the monitor (even if an exception is thrown).

---

## 6. ReentrantLock

Provides more control than `synchronized`.

```java
Lock lock = new ReentrantLock();
lock.lock();
try {
    // critical section
} finally {
    lock.unlock();
}
```

Useful methods:
```java
lock.tryLock()
lock.tryLock(5, TimeUnit.SECONDS)
lock.lockInterruptibly()
```

**tryLock()** — allows a thread to attempt to acquire a lock without waiting indefinitely.

```
Thread
  ↓
tryLock()
  ↓
Got lock? ── Yes → execute
       │
       No
       ↓
    do something else
```

---

## 7. wait() / notify() / notifyAll()

Used for thread communication.

**wait()**
- Releases the object's monitor.
- Moves the thread into a waiting state.
- The thread waits until it is notified/interrupted.

**notify()**
- Wakes one thread waiting on that object's monitor.

**notifyAll()**
- Wakes all threads waiting on that object's monitor (safer default — avoids missed wake-ups when multiple distinct conditions share one lock).

```java
synchronized (lock) {
    lock.wait();
}
```

```java
synchronized (lock) {
    lock.notify();
}
```

> **Important:** `notify()` does not directly give the lock to the waiting thread. The notified thread must reacquire the monitor lock before continuing.

```
Thread 1
   ↓
wait()
   ↓
releases lock
   ↓
WAITING
       ...
Thread 2
   ↓
notify()
   ↓
Thread 1 becomes eligible
   ↓
must reacquire lock
   ↓
continues
```

**Full producer-consumer walkthrough** — note that Thread 1 doesn't run the instant `notify()` is called; it only becomes eligible once Thread 2 actually releases the lock (by exiting the `synchronized` block):

```
SHARED OBJECT: lock
                         │
              ┌──────────┴──────────┐
              │                     │
          THREAD 1              THREAD 2
          Consumer               Producer
              │                     │
              ▼                     │
       acquire monitor lock         │
              │                     │
              ▼                     │
       check dataAvailable          │
              │                     │
          false                     │
              │                     │
              ▼                     │
          wait()                    │
              │                     │
              ├── releases lock ────┤
              │                     │
              ▼                     ▼
          WAITING            acquires monitor lock
                                    │
                                    ▼
                            produces data
                                    │
                                    ▼
                            dataAvailable = true
                                    │
                                    ▼
                                notify()
                                    │
                                    ▼
                          Thread 1 is WOKEN UP
                                    │
                         Thread 2 still owns lock
                                    │
                                    ▼
                           Thread 2 exits
                           synchronized block
                                    │
                                    ▼
                            releases lock
                                    │
                                    ▼
                         Thread 1 reacquires lock
                                    │
                                    ▼
                              RUNNABLE
                                    │
                                    ▼
                              RUNNING
                                    │
                                    ▼
                             consumes data
```

> This is why `wait()` must always be called in a loop that re-checks the condition (`while (!dataAvailable) wait();`, not `if`) — by the time Thread 1 reacquires the lock, another thread could have already consumed the data or the condition could no longer hold (spurious wakeups are also possible).

---

## 8. sleep() vs wait()

**Thread.sleep()**
```java
Thread.sleep(5000);
```
- Pauses the current thread.
- Does **not** release a monitor lock it already holds.
- Static method, works on any thread, no lock required.

**wait()**
```java
lock.wait();
```
- Releases the monitor.
- Thread enters waiting state.
- Requires ownership of that object's monitor (must be called inside `synchronized`).

> `sleep` = pause; `wait` = release monitor and wait for notification.

---

## 9. Deadlock

Occurs when threads wait indefinitely for locks held by each other.

```
Thread 1
   ↓
Lock A
   ↓
waiting for Lock B
         ↑
         │
Thread 2
   ↓
Lock B
   ↓
waiting for Lock A
```

Neither can proceed.

**Step-by-step formation:**

```
              LOCK A                    LOCK B
                │                          │
          ┌─────┴─────┐              ┌─────┴─────┐
          │           │              │           │
      THREAD 1                            THREAD 2
          │                                    │
          ▼                                    ▼
     acquires Lock A                    acquires Lock B
          │                                    │
          ▼                                    ▼
      does work...                        does work...
          │                                    │
          ▼                                    ▼
   needs Lock B next               needs Lock A next
          │                                    │
          ▼                                    ▼
    Lock B held by                     Lock A held by
      Thread 2 → BLOCKED                 Thread 1 → BLOCKED
          │                                    │
          └──────────── neither releases ──────┘
                              │
                              ▼
                         DEADLOCK
```

**Prevention**
- Consistent lock ordering.
- Avoid unnecessary nested locks.
- Use `tryLock()` with timeout.
- Keep critical sections small.

---

## 10. Livelock & Starvation

**Livelock** — threads keep changing state in response to each other but none make progress (e.g. two people repeatedly stepping aside for each other in a hallway).

```
Thread 1 → yields for Thread 2 → Thread 2 yields for Thread 1 → repeat forever
```

**Starvation** — a thread is perpetually denied access to a resource because other threads keep getting priority (e.g. a low-priority thread starved by a flood of high-priority ones, or an `unfair` lock always favoring recently-active threads).

| Problem | Threads blocked? | Threads doing work? |
|---|---|---|
| Deadlock | Yes, permanently | No |
| Livelock | No | Yes, but no progress |
| Starvation | No | Yes, but one thread never gets a turn |

---

## 11. volatile

`volatile` primarily provides **visibility**.

```java
volatile boolean running = true;
```

If Thread 1 changes `running = false;`, other threads can see the updated value immediately (it's not cached in a CPU register/thread-local cache).

> **Important:** `volatile` does not make compound operations atomic.

This is **not** safe:
```java
volatile int count;
count++; // read → modify → write, not atomic
```

---

## 12. Atomic Classes & CAS

For simple atomic operations:
```java
AtomicInteger counter = new AtomicInteger();
counter.incrementAndGet();
```

Atomic classes use low-level mechanisms such as **CAS — Compare-And-Swap**.

```
Current value == expected value?
        ↓
      YES
        ↓
Update value
```

If another thread changed it first, the CAS operation fails/retries (spins) instead of blocking.

Good for counters and similar atomic state updates. Common classes: `AtomicInteger`, `AtomicLong`, `AtomicBoolean`, `AtomicReference`.

---

## 13. Thread Pool

A thread pool contains reusable worker threads.

Instead of:
```
100 tasks
 ↓
create 100 threads
```

use:
```
100 tasks
   ↓
Thread Pool
   ↓
5 worker threads
   ↓
remaining tasks → work queue
```

When a worker finishes:
```
Worker
  ↓
finishes Task 1
  ↓
takes Task 6
  ↓
executes Task 6
```

**Benefits**
- Reuses threads.
- Avoids excessive thread creation.
- Controls concurrency.
- Manages thread lifecycle.

---

## 14. ExecutorService

Used to manage thread pools.

```java
ExecutorService executor = Executors.newFixedThreadPool(5);
```

Submit tasks:
```java
executor.execute(() -> doWork());
```
or:
```java
Future<Result> future = executor.submit(() -> doWork());
```

**execute() vs submit()**

| | `execute(Runnable)` | `submit(Runnable/Callable)` |
|---|---|---|
| Return value | none | returns a `Future` |
| Exception handling | thread's uncaught exception handler | captured in the `Future`, surfaces on `get()` |

**Internal working of `ThreadPoolExecutor`** (what every `Executors.new...()` factory wraps) — the order a submitted task is routed through:

```
                     task submitted
                          │
                          ▼
              ┌───────────────────────┐
              │  poolSize < corePoolSize? │
              └───────────┬───────────┘
                          │
                 ┌────────┴────────┐
                YES                NO
                 │                  │
                 ▼                  ▼
        spawn a new CORE      try to enqueue task
        thread, run task      into the work BlockingQueue
                                     │
                          ┌──────────┴──────────┐
                       queue has              queue is FULL
                       space → task waits           │
                          │                          ▼
                          ▼                 poolSize < maximumPoolSize?
                  idle worker thread                 │
                  picks it up when free      ┌────────┴────────┐
                          │                  YES                NO
                          ▼                   │                  │
                    executes task             ▼                  ▼
                                      spawn a new EXTRA    RejectedExecutionHandler
                                      thread, run task      (AbortPolicy by default
                                                              → throws exception)
```

> Sizing knobs: `corePoolSize`, `maximumPoolSize`, `workQueue` (capacity), `keepAliveTime` (how long idle extra-threads above core size stay alive before being reclaimed), `RejectedExecutionHandler` (Abort / CallerRuns / Discard / DiscardOldest).

---

## 15. Types of Executors

`Executors` factory methods (know the trade-offs — most are discouraged in production in favor of manually configured `ThreadPoolExecutor`):

| Factory | Behavior |
|---|---|
| `newFixedThreadPool(n)` | Fixed number of threads, unbounded queue |
| `newCachedThreadPool()` | Unbounded threads, reuses idle ones, can explode thread count under load |
| `newSingleThreadExecutor()` | One worker thread, tasks run sequentially in order |
| `newScheduledThreadPool(n)` | For delayed/periodic tasks (`scheduleAtFixedRate`, `scheduleWithFixedDelay`) |
| `newWorkStealingPool()` | Backed by `ForkJoinPool`, threads steal work from each other's queues |
| `newVirtualThreadPerTaskExecutor()` | (Java 21+) One cheap virtual thread per task |

> Interview trap: `newFixedThreadPool`/`newCachedThreadPool` use unbounded queues/thread creation — can cause `OutOfMemoryError` under sustained load. Prefer explicit `ThreadPoolExecutor` with bounded queues + rejection policy in production.

---

## 16. Runnable vs Callable

**Runnable** — doesn't return a result.
```java
Runnable task = () -> {
    System.out.println("working");
};
```

**Callable** — returns a result and can throw checked exceptions.
```java
Callable<Integer> task = () -> 10 + 20;
```

Result:
```java
Future<Integer> future = executor.submit(task);
Integer result = future.get();
```

---

## 17. Future

`Future` represents the result of an asynchronous computation.

```java
Future<Integer> future = executor.submit(() -> calculate());
```

Calling `future.get()` **blocks** the calling thread until the result is available.

> **Important:** `Future` tasks can still execute concurrently if the executor has multiple worker threads. `Future` doesn't force sequential execution — only `get()` is blocking, and only for the caller.

Limitations vs `CompletableFuture`: no chaining/composition, no manual completion, `get()` is the only way to consume the result (blocking).

**Two threads' worth of timeline** — the caller and the worker running independently, meeting only at `get()`:

```
MAIN THREAD                          WORKER THREAD (pool)
     │
     ▼
executor.submit(task) ────────────────────► picks up task
     │                                            │
     ▼                                            ▼
Future<Integer> returned                    executes calculate()
  (immediately, non-blocking)                     │
     │                                       (takes, say, 3s)
     ▼                                            │
does other work...                                │
     │                                            │
     ▼                                            │
future.get() called                               │
     │                                            │
     ▼                                            ▼
   BLOCKS ─────────────────────────────────  task completes
     │                                       result stored in Future
     │◄───────────────────────────────────────────┘
     ▼
  unblocks, returns result
     │
     ▼
continues with result
```

> If `get()` is called *after* the worker already finished, it returns immediately — no blocking. The blocking only happens if the caller gets there first.

---

## 18. CompletableFuture

Provides richer asynchronous composition.

```java
CompletableFuture<User> user = CompletableFuture.supplyAsync(() -> getUser());
```

Useful methods:
```java
supplyAsync()
thenApply()
thenCompose()
thenAccept()
allOf()
exceptionally()
handle()
```

Example:
```java
CompletableFuture.allOf(f1, f2, f3).join();
```

If tasks take:
```
Task 1 → 2 sec
Task 2 → 3 sec
Task 3 → 2 sec
```
and execute concurrently:
```
Total ≈ 3 seconds
```

**Timeline on the pool's worker threads:**

```
time →   0s        1s        2s        3s
         │         │         │         │
Task 1   ├───────────────────┤
         supplyAsync          done (2s)
         │
Task 2   ├─────────────────────────────┤
         supplyAsync                    done (3s)
         │
Task 3   ├───────────────────┤
         supplyAsync          done (2s)
         │
         ▼
   CompletableFuture.allOf(f1, f2, f3)
         │
         ▼
   waits for the SLOWEST of the three
         │
         ▼
      .join() unblocks at 3s
```

**thenApply vs thenCompose** (same idea as `map` vs `flatMap`):
- `thenApply(fn)` — `fn` returns a plain value.
- `thenCompose(fn)` — `fn` returns another `CompletableFuture`, and it's flattened (avoids `CompletableFuture<CompletableFuture<T>>`).

**Async chaining flow** — each stage can run on a different pool thread (no thread is held blocked between stages):

```
CompletableFuture
.supplyAsync(() -> getUser())
        │
        ▼
┌───────────────────┐
│  Pool Thread A     │
│  fetches User      │
└─────────┬──────────┘
          │  User
          ▼
.thenApply(user -> user.getName())
          │
          ▼
┌───────────────────┐
│  same/other thread │
│  transforms value  │
└─────────┬──────────┘
          │  String (name)
          ▼
.thenCompose(name -> orderService.findOrdersAsync(name))
          │
          ▼
┌────────────────────┐
│  Pool Thread B      │
│  starts NEW async   │
│  call, flattens the │
│  nested Future      │
└─────────┬───────────┘
          │  List<Order>   (unwrapped, not
          │                CompletableFuture<List<Order>>)
          ▼
.thenAccept(orders -> print(orders))
          │
          ▼
┌────────────────────┐
│  final callback     │
│  consumes result,   │
│  returns nothing    │
└─────────────────────┘
          │
          ▼
.exceptionally(ex -> fallback())
   ▲ catches any exception thrown at ANY earlier stage
```

> No stage here calls `get()`/`join()` — the calling thread that built this pipeline returns immediately after `supplyAsync()`; each `.then...()` callback just gets scheduled to run when its predecessor completes.

> **Important:** `CompletableFuture` doesn't automatically mean "non-blocking." `get()` and `join()` can block the calling thread. It only becomes non-blocking if you stay in the async callback chain (`thenApply`/`thenCompose`/...) instead of calling `get()`/`join()`.

---

## 19. Concurrency vs Parallelism

**Concurrency** — multiple tasks are in progress during the same period (may interleave on one core).
```
Task A → Task B → Task C → Task A
       context switching
```

**Parallelism** — multiple tasks actually execute simultaneously on different CPU cores.
```
Core 1 → Task A
Core 2 → Task B
Core 3 → Task C
```

A system can have both concurrency and parallelism.

**Example: 3 threads (T1, T2, T3) each counting to a million**

*Concurrency — single CPU core, OS time-slices between threads:*
```
Core 1 (the only core)

time →   0ms      10ms     20ms     30ms     40ms
         │         │         │         │        │
         ████████  ░░░░░░░░  ░░░░░░░░  ████████
  T1     RUNNING   paused    paused    RUNNING...
         ░░░░░░░░  ████████  ░░░░░░░░  ░░░░░░░░
  T2     paused    RUNNING   paused    paused
         ░░░░░░░░  ░░░░░░░░  ████████  ░░░░░░░░
  T3     paused    paused    RUNNING   paused

At any single instant, only ONE thread is actually executing.
The OS scheduler context-switches so fast it *looks* simultaneous.
```

*Parallelism — 3 CPU cores, threads genuinely run at the same instant:*
```
Core 1  T1  ████████████████████████████████████►  never paused
Core 2  T2  ████████████████████████████████████►  never paused
Core 3  T3  ████████████████████████████████████►  never paused

time →  0ms                                    40ms

All three threads are physically executing at the exact same
nanosecond, on three separate cores. No context switching needed.
```

> A quad-core laptop running 20 browser tabs is doing **both**: 4-way parallelism (4 tabs truly simultaneous, one per core) plus concurrency (the OS time-slices the remaining 16 tabs across those same 4 cores).

---

## 20. Blocking vs Non-blocking I/O

**Blocking**
```
Thread
  ↓
DB call
  ↓
WAIT
  ↓
DB response
  ↓
continue
```
The thread remains occupied while waiting.

**Non-blocking**
```
Event loop
   ↓
DB request
   ↓
thread handles other work
   ↓
DB response
   ↓
reactive pipeline continues
```

**Example: 3 requests hit an endpoint that calls a DB taking 2 seconds**

*Blocking — Spring MVC + JDBC, thread-per-request, pool size = 2:*
```
Thread Pool (2 threads)

time →     0s        1s        2s        3s        4s
           │         │         │         │         │
Thread-1   Req A ─────blocked on DB call─────► response sent (2s)
           │
Thread-2   Req B ─────blocked on DB call─────► response sent (2s)
           │
Req C      WAITING for a free thread ...........►
                                              Thread-1 freed, picks up Req C
                                              Req C ──blocked on DB call──► response sent (4s)
```
Req C can't even *start* until a thread frees up — throughput is capped by pool size, and every occupied thread sits idle (doing nothing) for the full 2s it's blocked.

*Non-blocking — WebFlux + R2DBC, 1 event-loop thread handles all three:*
```
Event Loop Thread (1 thread)

time →    0s              1s              2s
          │               │               │
Req A  →  DB call dispatched, thread moves on immediately
Req B  →  DB call dispatched, thread moves on immediately
Req C  →  DB call dispatched, thread moves on immediately
          │
          thread is FREE this whole time — not blocked on any of the 3 calls
          │
          ▼ (all three DB calls resolve around the same time)
2s:    DB response A arrives → event loop resumes Req A's pipeline → response sent
2s:    DB response B arrives → event loop resumes Req B's pipeline → response sent
2s:    DB response C arrives → event loop resumes Req C's pipeline → response sent
```
All three requests finish in ≈2s using a **single** thread — no request had to wait for a thread to free up, because no thread was ever occupied waiting on the DB in the first place.

---

## 21. WebFlux + R2DBC

Typical non-blocking stack:
```
WebFlux
   ↓
Netty
   ↓
Event Loop
   ↓
R2DBC
   ↓
PostgreSQL
```

The database may still take 5 seconds. WebFlux doesn't make the DB faster — it means the application thread doesn't sit blocked waiting for those 5 seconds.

> **Important:** WebFlux + JDBC is still blocking. JDBC itself is a blocking API; you need R2DBC (or another reactive driver) for true end-to-end non-blocking I/O.

**Full request lifecycle** — from HTTP request in to HTTP response out, showing exactly where the event-loop thread is freed up:

```
                  HTTP REQUEST
                      │
                      ▼
              ┌───────────────┐
              │ Netty Server  │
              └───────┬───────┘
                      │
                      ▼
              ┌───────────────┐
              │ Event Loop    │
              │    Thread     │
              └───────┬───────┘
                      │
                      ▼
              ┌───────────────┐
              │  Controller   │
              │               │
              │ Mono<User>    │
              └───────┬───────┘
                      │
                      ▼
             Reactive Pipeline
              (built, not run)
                      │
                      ▼
            WebFlux subscribes
              to Mono/Flux
                      │
                      ▼
                subscribe()
                      │
                      ▼
              Pipeline executes
                      │
                      ▼
             ┌────────────────┐
             │ R2DBC Database │
             │   Operation    │
             └───────┬────────┘
                      │
              Non-blocking I/O
                      │
                      ▼
              Event loop thread
              can do other work
                      │
                      ▼
               DB response
                      │
                      ▼
             Mono emits User
                      │
                      ▼
           Reactive pipeline
               continues
                      │
                      ▼
              WebFlux creates
              HTTP response
                      │
                      ▼
                HTTP RESPONSE
```

> The key win: between "Non-blocking I/O" and "DB response", the event-loop thread is **not** parked waiting — it's free to service other requests. That's the whole point of the reactive stack.

**Netty's boss/worker event-loop groups** — how one small pool of threads serves many concurrent connections:

```
                    ┌─────────────────────────┐
                    │   BOSS EventLoopGroup    │
                    │   (usually 1 thread)     │
                    │   accepts new TCP        │
                    │   connections             │
                    └────────────┬─────────────┘
                                 │ hands off accepted socket
                                 ▼
                    ┌─────────────────────────┐
                    │  WORKER EventLoopGroup   │
                    │  (≈ number of CPU cores) │
                    └────────────┬─────────────┘
                                 │
              ┌──────────────────┼──────────────────┐
              ▼                  ▼                   ▼
        EventLoop 1         EventLoop 2         EventLoop 3
      (1 thread, runs      (1 thread, runs     (1 thread, runs
       its own channels'    its own channels'    its own channels'
       I/O + callbacks)     I/O + callbacks)      I/O + callbacks)
              │                  │                   │
      Conn A, Conn D      Conn B, Conn E       Conn C, Conn F
      (pinned for their   (pinned for their    (pinned for their
       whole lifetime)     whole lifetime)       whole lifetime)
```

- A connection is **pinned** to one event-loop thread for its lifetime — all its I/O callbacks run on that same thread, so no locking is needed per-connection.
- With, say, 4 worker threads, thousands of connections are multiplexed — each thread runs a non-blocking select-loop and only does work when a socket actually has data ready, instead of one-thread-per-connection.
- This is *why* blocking a WebFlux handler (e.g. calling blocking JDBC on an event-loop thread) is so damaging — it doesn't just stall one request, it stalls every other connection pinned to that same event-loop thread.

**Reactive Streams protocol underneath `subscribe()`** — the actual signal exchange between publisher and subscriber (this is what makes backpressure possible):

```
SUBSCRIBER (e.g. WebFlux's response writer)     PUBLISHER (your Mono/Flux pipeline)
        │                                                │
        ▼                                                │
  .subscribe(subscriber) ─────────────────────────────►  │
        │                                                ▼
        │                                        onSubscribe(subscription)
        │◄───────────────────────────────────────────────┘
        ▼
  subscription.request(n) ──────────────────────────►  (demand signaled,
        │                                                n items or Long.MAX_VALUE)
        │                                                │
        │                                                ▼
        │                                          produces item
        │◄──────────────────────────────  onNext(item) ┘
        ▼
  handles item, may
  request(n) again
        │                                                │
        │                                          (repeats until done)
        │                                                ▼
        │◄──────────────────────────── onComplete() / onError(ex)
        ▼
     stream ends
```

> This `request(n)` step is exactly what implements **backpressure** (see [§25](#25-backpressure)) — the subscriber controls how many items it's asked for, so a fast publisher can never overwhelm a slow subscriber.

---

## 22. Mono vs Flux

**Mono** — zero or one result:
```java
Mono<User>
```

**Flux** — zero to many results:
```java
Flux<User>
```

Think:
```
Mono → User
Flux → User → User → User → ...
```

---

## 23. Reactive Pipeline & subscribe()

Reactive operations are generally **lazy**.

```java
Mono.just("hello")
    .map(String::toUpperCase);
```

This builds a pipeline. Execution begins only when a subscriber subscribes.

```
BUILD
  ↓
Mono / Flux pipeline
  ↓
SUBSCRIBE
  ↓
EXECUTION
  ↓
EMIT RESULT
```

In Spring WebFlux, you normally don't call `subscribe()` manually — the framework subscribes to the `Mono`/`Flux` returned by your controller.

```java
@GetMapping("/users/{id}")
public Mono<User> getUser(long id) {
    return repository.findById(id);
}
```

---

## 24. map() vs flatMap()

**map()** — transforms one value into another value:
```java
Mono<String> name = user.map(User::getName);
```
```
Mono<User>
    ↓ map
Mono<String>
```

**flatMap()** — transforms a value into another `Mono`/`Flux` and flattens it (avoids `Mono<Mono<T>>`):
```java
user.flatMap(u -> orderRepository.findByUserId(u.getId()));
```
```
Mono<User>
    ↓ flatMap
Mono<Order>
```

---

## 25. Backpressure

Backpressure controls the flow when the producer can produce faster than the consumer can process.

```
Fast Producer
     ↓
10,000 items
     ↓
Slow Consumer
     ↓
Can process limited demand
```

Reactive Streams lets the subscriber communicate demand upstream. `limitRate()` provides more explicit control:
```java
Flux.range(1, 10000)
    .limitRate(10);
```

> **Important:** Backpressure does not simply mean "process exactly 10 items at a time." It's a demand/flow-control mechanism — the subscriber requests `n` items, and the publisher never sends more than requested.

---

## 26. ConcurrentHashMap

`HashMap` is not thread-safe.
```java
HashMap<Integer, String> map;
```
Multiple threads can access it concurrently without built-in coordination (can corrupt internal structure, cause infinite loops on resize in older JDKs).

`ConcurrentHashMap` is designed for concurrent access:
```java
ConcurrentHashMap<Integer, String> map;
```
It uses concurrency mechanisms such as CAS and fine-grained (bucket-level) synchronization.

**Important interview point** — thread-safe doesn't mean two threads can't overwrite the same key. This is still possible:
```java
map.put(1, "A");
map.put(1, "B");
// final value can be "B"
```
Thread safety means the map's **internal data structure** remains safe under concurrent access — not that business-level races are prevented.

For atomic "put only if absent":
```java
map.putIfAbsent(1, "A");
```
Also useful: `computeIfAbsent()`, `compute()`, `merge()` — all atomic per-key.

---

## 27. CountDownLatch

Used when a thread needs to wait until a fixed number of tasks/events complete.

```java
CountDownLatch latch = new CountDownLatch(3);
```

Workers:
```java
latch.countDown();
```

Waiting thread:
```java
latch.await();
```

```
Task 1 → countDown()
Task 2 → countDown()
Task 3 → countDown()
              ↓
          count = 0
              ↓
       waiting thread continues
```

It doesn't make tasks execute sequentially, and it's **one-shot** — the count can't be reset.

---

## 28. CyclicBarrier

Allows multiple threads to wait for each other at a synchronization point.

```java
CyclicBarrier barrier = new CyclicBarrier(3);
```

Each thread:
```java
barrier.await();
```

```
Thread 1 → await() ─┐
Thread 2 → await() ─┼→ All arrive
Thread 3 → await() ─┘
                       ↓
                 All continue
```

**Cyclic** means the barrier can be reused (unlike `CountDownLatch`), and it can optionally run a barrier action once all parties arrive.

**Difference**

| | Purpose |
|---|---|
| `CountDownLatch` | wait for events/tasks to finish (one-shot) |
| `CyclicBarrier` | threads wait for each other (reusable) |

---

## 29. Semaphore

Controls the number of threads that can access a resource concurrently.

```java
Semaphore semaphore = new Semaphore(5);
```

Five permits:
```
Thread 1 → permit
Thread 2 → permit
Thread 3 → permit
Thread 4 → permit
Thread 5 → permit
Thread 6 → WAIT
```

When a thread finishes:
```java
semaphore.release();
```
Another thread can acquire the permit.

> **Important:** A `Semaphore` provides permits; it doesn't create threads. It's often used to bound access to a resource pool (e.g. DB connections, rate limiting).

---

## 30. ReadWriteLock

Useful when reads are much more frequent than writes.

```java
ReadWriteLock lock = new ReentrantReadWriteLock();
```

Multiple readers:
```
Reader 1 ─┐
Reader 2 ─┼→ Read Lock
Reader 3 ─┘
```

Only one writer:
```
Writer
  ↓
Write Lock
  ↓
Exclusive access
```

Compared with a normal `ReentrantLock`, `ReadWriteLock` allows multiple readers concurrently (writers still get exclusive access, and no reader/writer overlap).

---

## 31. shutdown() vs shutdownNow()

**shutdown()**
```
No new tasks
     ↓
Existing tasks complete
     ↓
Executor terminates
```

**shutdownNow()** — attempts to interrupt running tasks and returns tasks that were queued but never started.

> **Important:** `shutdownNow()` does not guarantee that running tasks immediately stop — it just interrupts them; the task's code must respond to the interrupt (check `Thread.interrupted()` or let a blocking call throw `InterruptedException`).

Idiomatic shutdown pattern:
```java
executor.shutdown();
if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
    executor.shutdownNow();
}
```

---

## 32. ThreadLocal

Gives each thread its own independent copy of a variable.

```java
ThreadLocal<SimpleDateFormat> formatter =
    ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd"));
```

```
Thread 1 → own copy of value
Thread 2 → own copy of value
Thread 3 → own copy of value
```

Common uses: per-request context (user id, transaction, MDC/logging correlation id), non-thread-safe objects like `SimpleDateFormat`.

> **Trap:** In pooled-thread environments (thread pools, servlet containers), always call `remove()` in a `finally` block — otherwise stale data leaks into the next task that reuses the same pooled thread (a classic memory-leak / cross-request data-leak bug).

---

## 33. Fork/Join Framework

Designed for divide-and-conquer, CPU-bound parallel work using a pool of worker threads that **steal work** from each other's queues.

```java
ForkJoinPool pool = new ForkJoinPool();
pool.invoke(new SumTask(array, 0, array.length));
```

```
Big Task
   ↓ fork
Task A   Task B
   ↓        ↓
smaller  smaller
tasks    tasks
   ↓        ↓
     join → combine result
```

- `fork()` — schedules a subtask asynchronously.
- `join()` — waits for and returns a subtask's result.
- Backs `parallelStream()` and `newWorkStealingPool()` internally.

---

## 34. BlockingQueue

A queue that blocks the producer when full and the consumer when empty — the standard building block for the producer-consumer pattern.

```java
BlockingQueue<Task> queue = new LinkedBlockingQueue<>(100);

// producer
queue.put(task);   // blocks if full

// consumer
Task t = queue.take(); // blocks if empty
```

```
Producer → put() → [ Queue ] → take() → Consumer
              blocks if full     blocks if empty
```

Common implementations: `ArrayBlockingQueue` (bounded, array-based), `LinkedBlockingQueue` (optionally bounded), `SynchronousQueue` (zero capacity, direct hand-off), `PriorityBlockingQueue`. `ThreadPoolExecutor`'s work queue is a `BlockingQueue`.

---

## 35. StampedLock

A newer, more scalable alternative to `ReadWriteLock` that adds an **optimistic read** mode (no lock actually taken — validated afterward).

```java
StampedLock lock = new StampedLock();

long stamp = lock.tryOptimisticRead();
int value = data; // read without locking
if (!lock.validate(stamp)) {
    stamp = lock.readLock(); // fall back to a real read lock
    try {
        value = data;
    } finally {
        lock.unlockRead(stamp);
    }
}
```

- Not reentrant (unlike `ReentrantLock`/`ReentrantReadWriteLock`) — re-locking from the same thread can deadlock.
- Can be faster than `ReadWriteLock` under heavy read contention since optimistic reads don't block writers at all.

---

## 36. Exchanger

A synchronization point where exactly two threads swap objects with each other.

```java
Exchanger<Buffer> exchanger = new Exchanger<>();

// Thread 1
Buffer full = exchanger.exchange(emptyBuffer);

// Thread 2
Buffer empty = exchanger.exchange(fullBuffer);
```

```
Thread 1 → exchange(A) ─┐
                         ├→ swap
Thread 2 → exchange(B) ─┘
   ↓                        ↓
gets B                   gets A
```

Used in producer-consumer pipelines that hand off buffers (e.g. filling one buffer while another is drained).

---

## 37. happens-before Relationship

The Java Memory Model (JMM) guarantees ordering/visibility only between actions connected by a **happens-before** relationship — without it, the compiler/CPU/JIT are free to reorder instructions and a thread may never observe another thread's writes.

Key happens-before rules:
- Actions in a thread happen-before every subsequent action in that **same thread** (program order).
- A `synchronized` block's unlock happens-before a later thread's lock on the same monitor.
- A write to a `volatile` field happens-before every subsequent read of that field.
- `Thread.start()` happens-before any action in the started thread.
- All actions in a thread happen-before another thread returns from `Thread.join()` on it.
- An action in a task submitted to an `Executor` happens-before actions in the thread that retrieves its `Future.get()` result.

> This is *the* underlying reason `volatile`, `synchronized`, and atomics matter — they're not just about mutual exclusion, they're about which writes are guaranteed to be visible to which threads at all.

---

## 38. Double-Checked Locking

A pattern to lazily initialize a singleton without paying the `synchronized` cost on every call — famous for being broken without `volatile`.

```java
public class Singleton {
    private static volatile Singleton instance;

    public static Singleton getInstance() {
        if (instance == null) {                 // 1st check (no lock)
            synchronized (Singleton.class) {
                if (instance == null) {          // 2nd check (locked)
                    instance = new Singleton();
                }
            }
        }
        return instance;
    }
}
```

> **Trap:** without `volatile`, another thread can see a **partially constructed** object due to instruction reordering (the reference can be published before the constructor finishes running). `volatile` establishes the happens-before edge that prevents this.

---

## 39. Virtual Threads (Project Loom, Java 21+)

Lightweight threads managed by the JVM (not the OS) — enable thousands/millions of concurrent blocking-style tasks without the memory/context-switch cost of platform (OS) threads.

```java
try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
    executor.submit(() -> handleRequest());
}
```

```
Platform Thread (OS-managed)          Virtual Thread (JVM-managed)
  heavyweight, ~MB stack                lightweight, grows as needed
  limited to ~thousands                 millions feasible
  1:1 with OS thread                    many virtual threads : few carrier (platform) threads
```

- Lets you write simple **blocking-style** code (no reactive chains) that still scales like non-blocking I/O — the JVM parks the virtual thread and frees the underlying carrier thread while it's blocked on I/O.
- `synchronized` blocks can still "pin" a virtual thread to its carrier thread in some JDK versions — prefer `ReentrantLock` in virtual-thread-heavy code for that reason.
- Not a replacement for reactive/WebFlux in all cases, but usually a simpler alternative for I/O-bound blocking workloads (e.g. traditional Spring MVC + JDBC at scale).

**Mount / unmount onto carrier threads** — this is the mechanism that lets a handful of OS threads serve thousands of "blocked" virtual threads:

```
        ForkJoinPool (carrier threads, ≈ CPU core count)
        ┌─────────────┐   ┌─────────────┐
        │ Carrier      │   │ Carrier      │
        │ Thread A     │   │ Thread B     │
        │ (OS thread)  │   │ (OS thread)  │
        └──────┬───────┘   └──────┬───────┘
               │ mounted            │ mounted
               ▼                    ▼
        Virtual Thread 1     Virtual Thread 4
        running normal            running normal
        Java code                  Java code
               │                    │
               ▼                    │
        calls blocking I/O          │
        (e.g. JDBC query,           │
         socket read)               │
               │                    │
               ▼                    │
     JVM UNMOUNTS VT1 from          │
     Carrier Thread A               │
     (VT1's stack saved to heap)    │
               │                    │
               ▼                    ▼
     Carrier Thread A is FREE   VT4 keeps running,
     → picks up next runnable    Carrier Thread B
       virtual thread (VT2,      untouched
       VT3, ...) from queue
               │
              ...
               ▼
     I/O completes for VT1
               │
               ▼
     VT1 re-queued as runnable
               │
               ▼
     JVM MOUNTS VT1 onto ANY
     free carrier thread
     (maybe A again, maybe not)
               │
               ▼
     VT1 resumes exactly where
     it left off
```

- **Mounting** = a virtual thread borrows a carrier (platform) thread to actually execute on the CPU.
- **Unmounting** = when the virtual thread blocks on I/O, the JVM detaches it from the carrier thread (saving its continuation/stack to the heap) and immediately hands that carrier thread to another runnable virtual thread.
- Thousands of virtual threads can be "blocked" in I/O simultaneously while only a handful of carrier threads actually exist — each carrier thread just keeps picking up whichever virtual thread is currently runnable.
- Contrast with platform threads: a blocked platform thread sits idle holding its OS thread hostage; a blocked virtual thread gives its carrier thread back immediately.

---

## 40. Common Interview Traps

**Is `volatile` atomic?**
No.
```java
volatile int count;
count++; // not atomic
```

**Does `notify()` release the lock?**
No. `wait()` releases the monitor. `notify()` wakes a waiting thread, but that thread must reacquire the monitor before continuing.

**Does `CompletableFuture` mean non-blocking?**
No.
```java
future.join();
future.get();
```
can block.

**Does WebFlux automatically make JDBC non-blocking?**
No.
```
WebFlux + JDBC   → blocking
WebFlux + R2DBC  → non-blocking I/O
```

**Does `ConcurrentHashMap` prevent overwrites?**
No. It provides thread-safe map operations, not automatic business-level conflict prevention.

**Does `Future` mean sequential execution?**
No. With `ExecutorService` and multiple worker threads, multiple `Future` tasks can execute concurrently.

**Does `shutdownNow()` instantly stop running tasks?**
No. It only interrupts them — the task must cooperate by checking the interrupt status.

**Is `StringBuilder` thread-safe?**
No — use `StringBuffer` (synchronized, slower) if thread safety across the same instance is genuinely needed, otherwise keep `StringBuilder` thread-confined.

**Does a bigger thread pool always mean better throughput?**
No — for CPU-bound work, more threads than CPU cores just adds context-switching overhead. Thread pool sizing should match the workload (CPU-bound ≈ core count; I/O-bound can go higher).

---

## ⭐ 41. Most Important Topics to Revise

If your interview is tomorrow, prioritize these:

1. Thread lifecycle
2. Race condition
3. `synchronized`
4. Monitor lock
5. `ReentrantLock`
6. `volatile`
7. `AtomicInteger` / CAS
8. `wait` / `notify`
9. Deadlock (+ livelock/starvation)
10. `ExecutorService`
11. Thread pool (+ types of executors)
12. `Future`
13. `CompletableFuture`
14. `CountDownLatch`
15. `CyclicBarrier`
16. `Semaphore`
17. `ConcurrentHashMap`
18. `ReadWriteLock`
19. Blocking vs non-blocking I/O
20. WebFlux + R2DBC
21. `Mono` / `Flux`
22. `map` / `flatMap`
23. `subscribe()`
24. Backpressure
25. Concurrency vs parallelism
26. `ThreadLocal`
27. `happens-before` / JMM
28. Virtual threads (if targeting Java 21+ roles)

---

<p align="center">⭐ If you find this repo helpful, please consider giving it a star! ⭐</p>
