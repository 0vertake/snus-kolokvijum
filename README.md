# Industrial Processing System

Kolokvijum 1 — Softverski nadzorno-upravljački sistemi (2026)

A thread-safe industrial job processing system built in C# (.NET 9) with async execution, priority queuing, event-driven architecture, and automatic reporting.

---

## Architecture

```
Main Thread (N Producer Threads)
        |
     Submit(Job)
        |
  ProcessingSystem
  ┌─────────────────────────────┐
  │  Thread-Safe Priority Queue │
  │  MaxQueueSize Limit         │
  │  Idempotency Check          │
  └─────────────────────────────┘
        |
      Dequeue
       /    \
Prime Job    IO Job
(CPU-Bound)  (Simulated IO Delay)
       \    /
  JobCompleted / JobFailed Event
        |
  TaskCompletionSource<int>
        |
     JobHandle
   (await Result)
```

---

## Features

- **Priority queue** — lower priority number = processed first
- **MaxQueueSize** — new jobs are rejected when the queue is full
- **Idempotency** — same `Job.Id` cannot be submitted or executed twice
- **Async execution** — jobs run on worker tasks, callers get a `JobHandle` with an awaitable `Task<int>`
- **Retry logic** — jobs that exceed 2s are retried up to 3 times total; on final failure, `ABORT` is written to the log
- **Event system** — `JobCompleted` and `JobFailed` events with async log writing
- **Periodic XML reports** — every 1 minute, circular buffer of 10 files
- **Time-independent design** — no `Thread.Sleep` for waiting; uses `TaskCompletionSource`

---

## Job Types

| Type | Description | Payload Format |
|------|-------------|----------------|
| `Prime` | Counts primes up to N using parallel computation | `numbers:<N>,threads:<T>` (threads clamped to [1, 8]) |
| `IO` | Simulates IO delay, returns random value 0–100 | `delay:<ms>` |

---

## Configuration

Edit `SystemConfig.xml` to configure the system:

```xml
<?xml version="1.0" encoding="utf-8"?>
<SystemConfig>
    <WorkerCount>5</WorkerCount>
    <MaxQueueSize>100</MaxQueueSize>
    <Jobs>
        <Job Type="Prime" Payload="numbers:10_000,threads:3" Priority="1"/>
        <Job Type="IO"    Payload="delay:1_000"              Priority="3"/>
    </Jobs>
</SystemConfig>
```

- `WorkerCount` — number of worker tasks processing the queue (also used as producer thread count)
- `MaxQueueSize` — max jobs allowed in the queue at once
- `Jobs` — initial jobs loaded on startup

---

## Running

Requires [.NET 9 SDK](https://dotnet.microsoft.com/download).

```bash
dotnet run
```

Press **Enter** to stop. Logs are written to `processing.log`. Reports are written to `report_00.xml` through `report_09.xml`.

---

## Output

**Console:**
```
[Producer 2] Submitted Prime priority=1 payload="numbers:15000,threads:4"
[2026-04-17 10:23:01.443] [COMPLETED] 3f2a..., 1741
[Producer 2] Job done -> result=1741
```

**processing.log:**
```
[2026-04-17 10:23:01.443] [COMPLETED] 3f2a1b..., 1741
[2026-04-17 10:23:03.112] [FAILED] 9c4e2a..., attempt 1
[2026-04-17 10:23:05.115] ABORT 9c4e2a...
```

**report_00.xml:**
```xml
<Report GeneratedAt="2026-04-17 10:24:00">
  <CompletedByType>
    <JobType Name="Prime" Count="12"/>
    <JobType Name="IO" Count="8"/>
  </CompletedByType>
  <AvgExecutionTimeByType>
    <JobType Name="Prime" AvgMs="843.25"/>
    <JobType Name="IO" AvgMs="1200.00"/>
  </AvgExecutionTimeByType>
  <FailedByType>
    <JobType Name="IO" Count="3"/>
  </FailedByType>
</Report>
```
