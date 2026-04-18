using IndustrialProcessingSystem.Models;
using IndustrialProcessingSystem.Services;

namespace Tests;

public class ProcessingSystemTests : IDisposable
{
    private readonly ProcessingSystem _system;
    private readonly string _logPath;

    public ProcessingSystemTests()
    {
        _logPath = Path.GetTempFileName();
        _system = new ProcessingSystem(workerCount: 1, maxQueueSize: 10, logPath: _logPath);
    }

    public void Dispose()
    {
        _system.Dispose();
        if (File.Exists(_logPath)) File.Delete(_logPath);
    }

    // --- Submit ---

    [Fact]
    public void Submit_ReturnsHandle_WhenJobIsNew()
    {
        var job = MakeJob(JobType.IO, "delay:50");
        var handle = _system.Submit(job);

        Assert.NotNull(handle);
        Assert.Equal(job.Id, handle.Id);
    }

    [Fact]
    public void Submit_ReturnsNull_WhenDuplicateId()
    {
        var job = MakeJob(JobType.IO, "delay:50");
        _system.Submit(job);

        var duplicate = _system.Submit(job);

        Assert.Null(duplicate);
    }

    [Fact]
    public void Submit_ReturnsNull_WhenQueueFull()
    {
        // workerCount: 0 so nothing drains the queue
        using var fullSystem = new ProcessingSystem(workerCount: 0, maxQueueSize: 2, logPath: _logPath);

        fullSystem.Submit(MakeJob(JobType.IO, "delay:99999"));
        fullSystem.Submit(MakeJob(JobType.IO, "delay:99999"));
        var overflow = fullSystem.Submit(MakeJob(JobType.IO, "delay:99999"));

        Assert.Null(overflow);
    }

    // --- Priority / query ---

    [Fact]
    public void GetTopJobs_ReturnsByPriority()
    {
        using var system = new ProcessingSystem(workerCount: 0, maxQueueSize: 10, logPath: _logPath);

        var low = MakeJob(JobType.IO, "delay:99999", priority: 5);
        var high = MakeJob(JobType.IO, "delay:99999", priority: 1);
        var mid = MakeJob(JobType.IO, "delay:99999", priority: 3);

        system.Submit(low);
        system.Submit(high);
        system.Submit(mid);

        var top = system.GetTopJobs(2).ToList();

        Assert.Equal(2, top.Count);
        Assert.Equal(high.Id, top[0].Id);
        Assert.Equal(mid.Id, top[1].Id);
    }

    [Fact]
    public void GetJob_ReturnsCorrectJob()
    {
        var job = MakeJob(JobType.IO, "delay:99999");
        using var system = new ProcessingSystem(workerCount: 0, maxQueueSize: 10, logPath: _logPath);
        system.Submit(job);

        var retrieved = system.GetJob(job.Id);

        Assert.NotNull(retrieved);
        Assert.Equal(job.Id, retrieved.Id);
    }

    [Fact]
    public void GetJob_ReturnsNull_WhenNotFound()
    {
        var result = _system.GetJob(Guid.NewGuid());
        Assert.Null(result);
    }

    // --- Execution ---

    [Fact]
    public async Task IOJob_CompletesWithResultInRange()
    {
        var job = MakeJob(JobType.IO, "delay:100");
        var handle = _system.Submit(job)!;

        var result = await handle.Result;

        Assert.InRange(result, 0, 100);
    }

    [Fact]
    public async Task PrimeJob_ReturnsCorrectPrimeCount()
    {
        // Primes up to 100: 2,3,5,7,11,13,17,19,23,29,31,37,41,43,47,53,59,61,67,71,73,79,83,89,97 = 25
        var job = MakeJob(JobType.Prime, "numbers:100,threads:2");
        var handle = _system.Submit(job)!;

        var result = await handle.Result;

        Assert.Equal(25, result);
    }

    [Fact]
    public async Task JobCompleted_EventFires_OnSuccess()
    {
        var tcs = new TaskCompletionSource<bool>();
        _system.JobCompleted += (_, _) => tcs.TrySetResult(true);

        var job = MakeJob(JobType.IO, "delay:50");
        _system.Submit(job);

        var fired = await Task.WhenAny(tcs.Task, Task.Delay(5_000)) == tcs.Task;
        Assert.True(fired, "JobCompleted event did not fire in time.");
    }

    // --- Helpers ---

    private static Job MakeJob(JobType type, string payload, int priority = 1) =>
        new() { Id = Guid.NewGuid(), Type = type, Payload = payload, Priority = priority };
}
