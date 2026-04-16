namespace IndustrialProcessingSystem.Models;

public class JobEventArgs : EventArgs
{
    public required Job Job { get; init; }
    public int Result { get; init; }
    public int Attempt { get; init; }
    public Exception? Exception { get; init; }
}
