namespace IndustrialProcessingSystem.Models;

public class JobHandle
{
    public Guid Id { get; init; }
    public Task<int> Result { get; init; } = Task.FromResult(0);
}
