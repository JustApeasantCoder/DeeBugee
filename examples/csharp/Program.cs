using DeeBugee.Extensions.Logging;
using Microsoft.Extensions.Logging;

var path = args.FirstOrDefault()
    ?? System.IO.Path.Combine(System.IO.Path.GetTempPath(), "ToolkitCSharpExample.jsonl");
using var factory = LoggerFactory.Create(builder =>
    builder.AddDeeBugee(new ToolkitLoggerOptions
    {
        Path = path,
        Source = "app"
    }));
var logger = factory.CreateLogger("example");
logger.LogInformation(
    "C# adapter example started in {duration_ms} ms with status {status}",
    8.5,
    200);
Console.WriteLine($"Wrote {path}");
