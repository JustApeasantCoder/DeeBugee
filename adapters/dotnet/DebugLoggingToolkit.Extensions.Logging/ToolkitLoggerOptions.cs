namespace DebugLoggingToolkit.Extensions.Logging;

public sealed class ToolkitLoggerOptions
{
    public required string Path { get; init; }
    public string Source { get; init; } = "app";
    public string AppSessionId { get; init; } = Guid.NewGuid().ToString("D");
    public int QueueCapacity { get; init; } = 16_384;
    public long RotationBytes { get; init; } = 50L * 1024 * 1024;
    public int ArchiveCount { get; init; } = 4;
}
