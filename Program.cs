using IndustrialProcessingSystem.Models;
using IndustrialProcessingSystem.Services;

string configPath = Path.Combine(AppContext.BaseDirectory, "SystemConfig.xml");
if (!File.Exists(configPath))
    configPath = "SystemConfig.xml";

var config = SystemConfigLoader.Load(configPath);
Console.WriteLine($"Config loaded: {config.WorkerCount} workers, max queue {config.MaxQueueSize}");

using var system = new ProcessingSystem(
    workerCount: config.WorkerCount,
    maxQueueSize: config.MaxQueueSize,
    logPath: "processing.log");

system.JobCompleted += async (_, args) =>
{
    try
    {
        string line = $"[{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}] [COMPLETED] {args.Job.Id}, {args.Result}";
        Console.WriteLine(line);
        await system.WriteLogAsync(line);
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[LOG ERROR] {ex.Message}");
    }
};

system.JobFailed += async (_, args) =>
{
    try
    {
        string line = $"[{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}] [FAILED] {args.Job.Id}, attempt {args.Attempt}";
        Console.WriteLine(line);
        await system.WriteLogAsync(line);
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[LOG ERROR] {ex.Message}");
    }
};

Console.WriteLine("\nInitial jobs from config:");
foreach (var job in config.InitialJobs)
{
    var handle = system.Submit(job);
    if (handle is not null)
        Console.WriteLine($"  Queued [{job.Type}] priority={job.Priority} payload=\"{job.Payload}\"");
    else
        Console.WriteLine($"  Rejected [{job.Type}] (duplicate or queue full)");
}

Console.WriteLine($"\nStarting {config.WorkerCount} producer threads...");
using var producerCts = new CancellationTokenSource();

var producers = Enumerable.Range(0, config.WorkerCount)
    .Select(id => Task.Run(() => ProducerLoop(id, system, producerCts.Token)))
    .ToArray();

Console.WriteLine("System running. Press Enter to stop...\n");
Console.ReadLine();

producerCts.Cancel();

try
{
    await Task.WhenAll(producers.Select(p => p.ContinueWith(_ => { })));
}
catch { }

Console.WriteLine("Shutting down...");

static void ProducerLoop(int producerId, ProcessingSystem system, CancellationToken ct)
{
    var rng = new Random(producerId * 31 + Environment.TickCount);

    while (!ct.IsCancellationRequested)
    {
        try
        {
            var type = rng.Next(2) == 0 ? JobType.Prime : JobType.IO;

            string payload = type == JobType.Prime
                ? $"numbers:{rng.Next(1_000, 30_000)},threads:{rng.Next(1, 9)}"
                : $"delay:{rng.Next(100, 4_000)}";

            var job = new Job
            {
                Id = Guid.NewGuid(),
                Type = type,
                Payload = payload,
                Priority = rng.Next(1, 6)
            };

            var handle = system.Submit(job);

            if (handle is not null)
            {
                Console.WriteLine(
                    $"[Producer {producerId}] Submitted {job.Type} priority={job.Priority} payload=\"{job.Payload}\"");

                _ = handle.Result.ContinueWith(t =>
                {
                    if (t.IsCompletedSuccessfully)
                        Console.WriteLine($"[Producer {producerId}] Job done -> result={t.Result}");
                    else if (t.IsFaulted)
                        Console.WriteLine($"[Producer {producerId}] Job aborted -> {t.Exception?.InnerException?.Message}");
                }, TaskScheduler.Default);
            }
            else
            {
                Console.WriteLine($"[Producer {producerId}] Queue full or duplicate -- skipping");
            }

            Thread.Sleep(rng.Next(300, 1_200));
        }
        catch (OperationCanceledException)
        {
            break;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Producer {producerId}] Error: {ex.Message}");
        }
    }

    Console.WriteLine($"[Producer {producerId}] Stopped.");
}
