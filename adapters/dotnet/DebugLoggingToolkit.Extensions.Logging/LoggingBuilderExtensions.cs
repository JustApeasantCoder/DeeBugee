using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DebugLoggingToolkit.Extensions.Logging;

public static class LoggingBuilderExtensions
{
    public static ILoggingBuilder AddDebugLoggingToolkit(
        this ILoggingBuilder builder,
        ToolkitLoggerOptions options)
    {
        builder.Services.AddSingleton<ILoggerProvider>(_ => new ToolkitLoggerProvider(options));
        return builder;
    }
}
