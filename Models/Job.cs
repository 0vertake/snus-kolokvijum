namespace IndustrialProcessingSystem.Models;

public class Job
{
    public Guid Id { get; init; } = Guid.NewGuid();
    public JobType Type { get; init; }
    public string Payload { get; init; } = string.Empty;
    public int Priority { get; init; }
}
