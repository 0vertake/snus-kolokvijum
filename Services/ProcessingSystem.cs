using System.Collections.Concurrent;
using System.Diagnostics;
using System.Xml.Linq;
using IndustrialProcessingSystem.Models;

namespace IndustrialProcessingSystem.Services;

public class JobStats
{
    private int _completed;
    private long _totalMs;
    private int _failed;

    public int Completed => _completed;
    public long TotalMs => _totalMs;
    public int Failed => _failed;

    public void RecordCompleted(long ms)
    {
        Interlocked.Increment(ref _completed);
        Interlocked.Add(ref _totalMs, ms);
    }

    public void RecordFailed() => Interlocked.Increment(ref _failed);
}

public class ProcessingSystem : IDisposable
{
    private readonly PriorityQueue<(Job job, TaskCompletionSource<int> tcs), int> _queue = new();
    private readonly object _queueLock = new();
    private readonly HashSet<Guid> _submittedIds = [];
    private readonly ConcurrentDictionary<Guid, Job> _allJobs = new();

    private readonly CancellationTokenSource _cts = new();
    private readonly List<Task> _workers;

    private readonly SemaphoreSlim _logLock = new(1, 1);
    private readonly string _logPath;

    private readonly ConcurrentDictionary<JobType, JobStats> _stats = new();
    private int _reportCounter;
    private readonly object _reportCounterLock = new();
    private readonly Timer _reportTimer;

    public event EventHandler<JobEventArgs>? JobCompleted;
    public event EventHandler<JobEventArgs>? JobFailed;

    private const int JobTimeoutMs = 2_000;
    private const int MaxAttempts = 3;

    public ProcessingSystem(int workerCount, int maxQueueSize, string logPath = "processing.log")
    {
        MaxQueueSize = maxQueueSize;
        _logPath = logPath;

        foreach (JobType t in Enum.GetValues<JobType>())
            _stats[t] = new JobStats();

        _workers = Enumerable
            .Range(0, workerCount)
            .Select(_ => Task.Run(() => WorkerLoop(_cts.Token)))
            .ToList();

        _reportTimer = new Timer(
            _ => _ = GenerateReportAsync(),
            null,
            TimeSpan.FromMinutes(1),
            TimeSpan.FromMinutes(1));
    }

    public int MaxQueueSize { get; }

    public JobHandle? Submit(Job job)
    {
        lock (_queueLock)
        {
            if (_submittedIds.Contains(job.Id))
                return null;

            if (_queue.Count >= MaxQueueSize)
                return null;

            _submittedIds.Add(job.Id);
            _allJobs[job.Id] = job;

            var tcs = new TaskCompletionSource<int>(
                TaskCreationOptions.RunContinuationsAsynchronously);

            _queue.Enqueue((job, tcs), job.Priority);

            return new JobHandle { Id = job.Id, Result = tcs.Task };
        }
    }

    public IEnumerable<Job> GetTopJobs(int n)
    {
        lock (_queueLock)
        {
            return _queue.UnorderedItems
                .OrderBy(x => x.Priority)
                .Take(n)
                .Select(x => x.Element.job)
                .ToList();
        }
    }

    public Job? GetJob(Guid id)
    {
        _allJobs.TryGetValue(id, out var job);
        return job;
    }

    private async Task WorkerLoop(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            (Job job, TaskCompletionSource<int> tcs) item = default;
            bool dequeued;

            lock (_queueLock)
            {
                dequeued = _queue.TryDequeue(out item, out _);
            }

            if (!dequeued)
            {
                try { await Task.Delay(50, ct); }
                catch (OperationCanceledException) { break; }
                continue;
            }

            await ProcessWithRetry(item.job, item.tcs, ct);
        }
    }

    private async Task ProcessWithRetry(
        Job job,
        TaskCompletionSource<int> tcs,
        CancellationToken globalCt)
    {
        Exception? lastEx = null;

        for (int attempt = 1; attempt <= MaxAttempts; attempt++)
        {
            using var primeCts = CancellationTokenSource.CreateLinkedTokenSource(globalCt);

            var sw = Stopwatch.StartNew();

            Task<int> jobTask = job.Type == JobType.Prime
                ? Task.Run(() => ExecutePrime(job.Payload, primeCts.Token), globalCt)
                : Task.Run(() => ExecuteIO(job.Payload), globalCt);

            var timeoutTask = Task.Delay(JobTimeoutMs, globalCt);
            Task winner;

            try
            {
                winner = await Task.WhenAny(jobTask, timeoutTask);
            }
            catch (OperationCanceledException)
            {
                tcs.TrySetCanceled(globalCt);
                return;
            }

            sw.Stop();

            if (winner == jobTask && jobTask.IsCompletedSuccessfully)
            {
                int result = jobTask.Result;
                _stats[job.Type].RecordCompleted(sw.ElapsedMilliseconds);
                tcs.TrySetResult(result);
                OnJobCompleted(job, result, attempt);
                return;
            }

            primeCts.Cancel();

            lastEx = jobTask.IsFaulted
                ? (jobTask.Exception?.InnerException ?? jobTask.Exception)
                : new TimeoutException($"Job {job.Id} timed out on attempt {attempt}.");

            OnJobFailed(job, attempt, lastEx);

            if (attempt == MaxAttempts)
            {
                _stats[job.Type].RecordFailed();
                await WriteLogAsync($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}] ABORT {job.Id}");
                tcs.TrySetException(lastEx ?? new Exception($"Job {job.Id} aborted after {MaxAttempts} attempts."));
            }
        }
    }

    private static int ExecutePrime(string payload, CancellationToken ct)
    {
        var parts = payload.Split(',');
        int upper = int.Parse(parts[0].Split(':')[1].Replace("_", ""));
        int threads = Math.Clamp(int.Parse(parts[1].Split(':')[1]), 1, 8);

        int count = 0;
        var opts = new ParallelOptions
        {
            MaxDegreeOfParallelism = threads,
            CancellationToken = ct
        };

        try
        {
            Parallel.For(2, upper + 1, opts, i =>
            {
                if (IsPrime(i))
                    Interlocked.Increment(ref count);
            });
        }
        catch (OperationCanceledException)
        {
            throw;
        }

        return count;
    }

    private static int ExecuteIO(string payload)
    {
        int delay = int.Parse(payload.Split(':')[1].Replace("_", ""));
        Thread.Sleep(delay);
        return Random.Shared.Next(0, 101);
    }

    private static bool IsPrime(int n)
    {
        if (n < 2) return false;
        if (n == 2) return true;
        if (n % 2 == 0) return false;
        for (int i = 3; (long)i * i <= n; i += 2)
            if (n % i == 0) return false;
        return true;
    }

    private void OnJobCompleted(Job job, int result, int attempt) =>
        JobCompleted?.Invoke(this, new JobEventArgs
        {
            Job = job,
            Result = result,
            Attempt = attempt
        });

    private void OnJobFailed(Job job, int attempt, Exception? ex) =>
        JobFailed?.Invoke(this, new JobEventArgs
        {
            Job = job,
            Attempt = attempt,
            Exception = ex
        });

    public async Task WriteLogAsync(string message)
    {
        await _logLock.WaitAsync();
        try
        {
            await File.AppendAllTextAsync(_logPath, message + Environment.NewLine);
        }
        finally
        {
            _logLock.Release();
        }
    }

    private async Task GenerateReportAsync()
    {
        int slot;
        lock (_reportCounterLock)
        {
            slot = _reportCounter % 10;
            _reportCounter++;
        }

        var snapshot = _stats.Select(kvp => new
        {
            Type = kvp.Key,
            kvp.Value.Completed,
            AvgMs = kvp.Value.Completed > 0
                ? (double)kvp.Value.TotalMs / kvp.Value.Completed
                : 0.0,
            kvp.Value.Failed
        }).ToList();

        var doc = new XDocument(
            new XDeclaration("1.0", "utf-8", null),
            new XElement("Report",
                new XAttribute("GeneratedAt", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")),
                new XElement("CompletedByType",
                    snapshot.Select(s =>
                        new XElement("JobType",
                            new XAttribute("Name", s.Type),
                            new XAttribute("Count", s.Completed)))),
                new XElement("AvgExecutionTimeByType",
                    snapshot.Select(s =>
                        new XElement("JobType",
                            new XAttribute("Name", s.Type),
                            new XAttribute("AvgMs", s.AvgMs.ToString("F2"))))),
                new XElement("FailedByType",
                    snapshot
                        .OrderBy(s => s.Type)
                        .Select(s =>
                            new XElement("JobType",
                                new XAttribute("Name", s.Type),
                                new XAttribute("Count", s.Failed))))));

        string reportPath = $"report_{slot:D2}.xml";
        doc.Save(reportPath);

        await WriteLogAsync($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}] [REPORT] Written to {reportPath}");
    }

    public void Dispose()
    {
        _reportTimer.Dispose();
        _cts.Cancel();
        Task.WaitAll([.. _workers.Select(w => w.ContinueWith(_ => { }))],
            TimeSpan.FromSeconds(5));
        _cts.Dispose();
        _logLock.Dispose();
        GC.SuppressFinalize(this);
    }
}
