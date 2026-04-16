using System.Xml.Linq;
using IndustrialProcessingSystem.Models;

namespace IndustrialProcessingSystem.Services;

public class SystemConfig
{
    public int WorkerCount { get; init; }
    public int MaxQueueSize { get; init; }
    public List<Job> InitialJobs { get; init; } = [];
}

public static class SystemConfigLoader
{
    public static SystemConfig Load(string path)
    {
        var doc = XDocument.Load(path);
        var root = doc.Root ?? throw new InvalidOperationException("Invalid config XML.");

        int workerCount = int.Parse(root.Element("WorkerCount")!.Value);
        int maxQueueSize = int.Parse(root.Element("MaxQueueSize")!.Value);

        var jobs = root.Element("Jobs")!
            .Elements("Job")
            .Select(e => new Job
            {
                Id = Guid.NewGuid(),
                Type = Enum.Parse<JobType>(e.Attribute("Type")!.Value, ignoreCase: true),
                Payload = e.Attribute("Payload")!.Value,
                Priority = int.Parse(e.Attribute("Priority")!.Value)
            })
            .ToList();

        return new SystemConfig
        {
            WorkerCount = workerCount,
            MaxQueueSize = maxQueueSize,
            InitialJobs = jobs
        };
    }
}
