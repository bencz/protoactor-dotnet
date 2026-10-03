using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;

namespace Proto.Cluster.PubSub.Tests;

/// <summary>
///     Captures the log lines written by <see cref="PubSubMemberDeliveryActor" />. Its logger is static and created on
///     first use, so the logger factory is registered when this test assembly loads, before any test runs.
/// </summary>
public static class DeliveryLogCapture
{
    public static readonly ConcurrentQueue<string> Lines = new();

    [ModuleInitializer]
    internal static void Register() =>
        Log.SetLoggerFactory(LoggerFactory.Create(builder => builder
            .SetMinimumLevel(LogLevel.Information)
            .AddFilter((category, _) => category == typeof(PubSubMemberDeliveryActor).FullName)
            .AddProvider(new CapturingProvider())));

    private sealed class CapturingProvider : ILoggerProvider
    {
        public ILogger CreateLogger(string categoryName) => new CapturingLogger();

        public void Dispose()
        {
        }
    }

    private sealed class CapturingLogger : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter) => Lines.Enqueue(formatter(state, exception));
    }
}
