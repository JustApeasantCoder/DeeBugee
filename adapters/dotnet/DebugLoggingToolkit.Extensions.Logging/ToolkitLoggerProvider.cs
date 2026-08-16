using System.Collections;
using System.Collections.Concurrent;
using System.Text.Json;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;

namespace DebugLoggingToolkit.Extensions.Logging;

public sealed class ToolkitLoggerProvider : ILoggerProvider
{
    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web);
    private readonly ToolkitLoggerOptions _options;
    private readonly Channel<ToolkitLogEvent> _channel;
    private readonly ConcurrentDictionary<string, ToolkitLogger> _loggers = new();
    private readonly CancellationTokenSource _shutdown = new();
    private readonly Task _writer;
    private long _dropped;

    public ToolkitLoggerProvider(ToolkitLoggerOptions options)
    {
        _options = options;
        _channel = Channel.CreateBounded<ToolkitLogEvent>(new BoundedChannelOptions(options.QueueCapacity)
        {
            SingleReader = true,
            FullMode = BoundedChannelFullMode.Wait
        });
        _writer = Task.Run(WriteLoopAsync);
    }

    public ILogger CreateLogger(string categoryName) =>
        _loggers.GetOrAdd(categoryName, category => new ToolkitLogger(this, category));

    internal void Write(ToolkitLogEvent entry)
    {
        ReportDropped();
        if (!_channel.Writer.TryWrite(entry)) Interlocked.Increment(ref _dropped);
    }

    public void Dispose()
    {
        ReportDropped();
        _channel.Writer.TryComplete();
        _shutdown.CancelAfter(TimeSpan.FromSeconds(5));
        try { _writer.GetAwaiter().GetResult(); } catch (OperationCanceledException) { }
        _shutdown.Dispose();
    }

    private void ReportDropped()
    {
        var count = Interlocked.Exchange(ref _dropped, 0);
        if (count == 0) return;
        var warning = new ToolkitLogEvent
        {
            Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            Level = "warn",
            Source = _options.Source,
            Subsystem = "logging",
            Event = "logger.events_dropped",
            Message = $"Dropped {count} log events because the writer queue was full",
            AppSessionId = _options.AppSessionId,
            Fields = new Dictionary<string, object?> { ["count"] = count }
        };
        if (!_channel.Writer.TryWrite(warning)) Interlocked.Add(ref _dropped, count);
    }

    private async Task WriteLoopAsync()
    {
        Directory.CreateDirectory(System.IO.Path.GetDirectoryName(_options.Path) ?? ".");
        var batch = new List<ToolkitLogEvent>(512);
        while (await _channel.Reader.WaitToReadAsync(_shutdown.Token).ConfigureAwait(false))
        {
            while (batch.Count < 512 && _channel.Reader.TryRead(out var entry)) batch.Add(entry);
            if (batch.Count == 0) continue;
            var lines = string.Concat(batch.Select(entry => JsonSerializer.Serialize(entry, SerializerOptions) + "\n"));
            await RotateIfNeededAsync(System.Text.Encoding.UTF8.GetByteCount(lines)).ConfigureAwait(false);
            await File.AppendAllTextAsync(_options.Path, lines, _shutdown.Token).ConfigureAwait(false);
            batch.Clear();
        }
    }

    private async Task RotateIfNeededAsync(int incomingBytes)
    {
        var currentBytes = File.Exists(_options.Path) ? new FileInfo(_options.Path).Length : 0;
        if (currentBytes == 0 || currentBytes + incomingBytes <= _options.RotationBytes) return;
        if (_options.ArchiveCount <= 0)
        {
            File.Delete(_options.Path);
            return;
        }
        File.Delete($"{_options.Path}.{_options.ArchiveCount}");
        for (var generation = _options.ArchiveCount - 1; generation >= 1; generation--)
        {
            var source = $"{_options.Path}.{generation}";
            if (File.Exists(source)) File.Move(source, $"{_options.Path}.{generation + 1}");
        }
        if (File.Exists(_options.Path)) File.Move(_options.Path, $"{_options.Path}.1");
        await Task.CompletedTask;
    }

    private sealed class ToolkitLogger(ToolkitLoggerProvider provider, string category) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => logLevel != LogLevel.None;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            if (!IsEnabled(logLevel)) return;
            var fields = ExtractFields(state);
            var subsystem = TakeString(fields, "subsystem") ?? category;
            var eventName = TakeString(fields, "event")
                ?? (eventId.Name ?? (eventId.Id != 0 ? $"event.{eventId.Id}" : "log.message"));
            provider.Write(new ToolkitLogEvent
            {
                Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                Level = LevelText(logLevel),
                Source = provider._options.Source,
                Subsystem = subsystem,
                Target = category,
                Event = eventName,
                Message = formatter(state, exception),
                AppSessionId = provider._options.AppSessionId,
                PlaybackSessionId = TakeString(fields, "playback_session_id"),
                RequestId = TakeString(fields, "request_id"),
                SessionId = TakeString(fields, "session_id"),
                Provider = TakeString(fields, "provider"),
                DurationMs = TakeDouble(fields, "duration_ms"),
                ErrorKind = TakeString(fields, "error_kind") ?? exception?.GetType().Name,
                Status = Take(fields, "status"),
                Fields = Redact(fields)
            });
        }
    }

    private static Dictionary<string, object?> ExtractFields<TState>(TState state)
    {
        if (state is not IEnumerable<KeyValuePair<string, object?>> values) return new();
        return values
            .Where(pair => pair.Key != "{OriginalFormat}")
            .ToDictionary(pair => pair.Key, pair => pair.Value, StringComparer.OrdinalIgnoreCase);
    }

    private static object? Take(Dictionary<string, object?> fields, string key)
    {
        if (!fields.Remove(key, out var value)) return null;
        return value;
    }

    private static string? TakeString(Dictionary<string, object?> fields, string key) =>
        Take(fields, key)?.ToString();

    private static double? TakeDouble(Dictionary<string, object?> fields, string key) =>
        double.TryParse(TakeString(fields, key), out var value) ? value : null;

    private static IReadOnlyDictionary<string, object?>? Redact(Dictionary<string, object?> fields)
    {
        if (fields.Count == 0) return null;
        var seen = new HashSet<object>(ReferenceEqualityComparer.Instance);
        return fields.ToDictionary(
            pair => pair.Key,
            pair => Sanitize(pair.Key, pair.Value, seen, 0),
            StringComparer.OrdinalIgnoreCase);
    }

    private static object? Sanitize(
        string key,
        object? value,
        HashSet<object> seen,
        int depth)
    {
        if (IsSensitiveKey(key)) return "[REDACTED]";
        if (value is null
            or string
            or bool
            or byte
            or sbyte
            or short
            or ushort
            or int
            or uint
            or long
            or ulong
            or float
            or double
            or decimal
            or DateTime
            or DateTimeOffset
            or Guid)
            return value;
        if (depth >= 8) return "[MaxDepth]";
        if (!value.GetType().IsValueType && !seen.Add(value)) return "[Circular]";

        if (value is Exception exception)
        {
            return new Dictionary<string, object?>
            {
                ["name"] = exception.GetType().Name,
                ["message"] = exception.Message,
                ["stack"] = exception.StackTrace
            };
        }
        if (value is IDictionary dictionary)
        {
            var result = new Dictionary<string, object?>();
            foreach (DictionaryEntry entry in dictionary)
            {
                var childKey = entry.Key?.ToString() ?? "value";
                result[childKey] = Sanitize(childKey, entry.Value, seen, depth + 1);
            }
            return result;
        }
        if (value is IEnumerable values)
        {
            return values.Cast<object?>()
                .Take(256)
                .Select(item => Sanitize("value", item, seen, depth + 1))
                .ToArray();
        }
        return value.ToString();
    }

    private static bool IsSensitiveKey(string key) =>
        System.Text.RegularExpressions.Regex.IsMatch(
            key,
            "token|secret|api.?key|authorization|cookie|passkey|signature|magnet",
            System.Text.RegularExpressions.RegexOptions.IgnoreCase);

    private static string LevelText(LogLevel level) => level switch
    {
        LogLevel.Trace => "trace",
        LogLevel.Debug => "debug",
        LogLevel.Information => "info",
        LogLevel.Warning => "warn",
        LogLevel.Error => "error",
        LogLevel.Critical => "fatal",
        _ => "info"
    };
}
