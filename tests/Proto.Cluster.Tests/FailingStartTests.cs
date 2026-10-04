#nullable enable
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using ClusterTest.Messages;
using FluentAssertions;
using Xunit;
using static Proto.TestKit.TestKit;

namespace Proto.Cluster.Tests;

public class RequestTimeoutTests
{
    [Theory]
    [InlineData(0, 1)]
    [InlineData(-1000, 1)]
    [InlineData(500, 1)]
    [InlineData(1000, 1)]
    [InlineData(1200, 2)]
    [InlineData(5000, 5)]
    public void RequestTimeoutIsRoundedUpToWholeSeconds(int milliseconds, int expectedSeconds) =>
        ClusterTimeouts.ToWholeSeconds(TimeSpan.FromMilliseconds(milliseconds))
            .Should().Be(expectedSeconds);
}

public class FailingStartClusterFixture : BaseInMemoryClusterFixture
{
    public FailingStartClusterFixture() : base(1)
    {
    }

    protected override ClusterKind[] ClusterKinds => new[]
    {
        new ClusterKind(FailingStartActor.Kind, Props.FromProducer(() => new FailingStartActor()))
    };
}

public class FailingStartTests : IClassFixture<FailingStartClusterFixture>
{
    private readonly FailingStartClusterFixture _fixture;

    public FailingStartTests(FailingStartClusterFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task VirtualActorThatFailsToStartIsDeactivatedInsteadOfRestartedInTheBackground()
    {
        var identity = $"always-fails-{Guid.NewGuid():N}";
        var member = _fixture.Members[0];
        var activationsBefore = member.GetClusterKind(FailingStartActor.Kind).Count;

        var request = () => member.RequestAsync<Pong>(identity, FailingStartActor.Kind,
            new Ping { Message = FailingStartActor.AlwaysFail },
            new CancellationTokenSource(FailingStartActor.StartDuration * 2.5).Token);

        await request.Should().ThrowAsync<TimeoutException>();

        // Wait for the last activation started by the request to finish failing
        await AwaitConditionAsync(() => member.GetClusterKind(FailingStartActor.Kind).Count == activationsBefore,
            TimeSpan.FromSeconds(5));

        var startsWhenTheCallerGaveUp = FailingStartActor.Starts.GetValueOrDefault(identity);

        // A restart loop would keep starting it every StartDuration even though nobody is asking for it
        await Task.Delay(FailingStartActor.StartDuration * 3);

        FailingStartActor.Starts.GetValueOrDefault(identity).Should().Be(startsWhenTheCallerGaveUp);
        member.GetClusterKind(FailingStartActor.Kind).Count.Should().Be(activationsBefore);
    }

    [Fact]
    public async Task StopHandlersDoNotRunAfterAFailedStart()
    {
        var identity = $"{FailingStartActor.AlwaysFailWithStopHandlers}-{Guid.NewGuid():N}";
        var member = _fixture.Members[0];
        var activationsBefore = member.GetClusterKind(FailingStartActor.Kind).Count;

        var request = () => member.RequestAsync<Pong>(identity, FailingStartActor.Kind,
            new Ping { Message = "hello" },
            new CancellationTokenSource(FailingStartActor.StartDuration * 1.5).Token);

        await request.Should().ThrowAsync<TimeoutException>();

        await AwaitConditionAsync(() => member.GetClusterKind(FailingStartActor.Kind).Count == activationsBefore,
            TimeSpan.FromSeconds(5));

        var startsWhenTheCallerGaveUp = FailingStartActor.Starts.GetValueOrDefault(identity);

        // A stop that failed in the stop handlers would leave the actor half stopped and restarting
        await Task.Delay(FailingStartActor.StartDuration * 3);

        FailingStartActor.Starts.GetValueOrDefault(identity).Should().Be(startsWhenTheCallerGaveUp);
        FailingStartActor.StopHandlerCalls.GetValueOrDefault(identity).Should().Be(0,
            "the grain never started, so there is no state for its stop handlers to clean up or save");
        member.GetClusterKind(FailingStartActor.Kind).Count.Should().Be(activationsBefore);
    }

    [Fact]
    public async Task VirtualActorIsActivatedAgainByTheNextRequestAfterAFailedStart()
    {
        var identity = $"fails-once-{Guid.NewGuid():N}";

        var pong = await _fixture.Members[0].RequestAsync<Pong>(identity, FailingStartActor.Kind,
            new Ping { Message = FailingStartActor.FailFirstStart },
            new CancellationTokenSource(TimeSpan.FromSeconds(15)).Token);

        pong!.Message.Should().Be(FailingStartActor.FailFirstStart);
        FailingStartActor.Starts.GetValueOrDefault(identity).Should().Be(2);
    }
}

public class FailingStartActor : IActor
{
    public const string Kind = "failing-start";
    public const string AlwaysFail = "always-fails";
    public const string FailFirstStart = "fails-once";
    public const string AlwaysFailWithStopHandlers = "always-fails-with-stop-handlers";

    // Longer than the supervision retry window divided by its retry count, which is what made restarts loop forever
    public static readonly TimeSpan StartDuration = TimeSpan.FromSeconds(1.2);

    public static readonly ConcurrentDictionary<string, int> Starts = new();
    public static readonly ConcurrentDictionary<string, int> StopHandlerCalls = new();

    private string? _state;

    public async Task ReceiveAsync(IContext context)
    {
        switch (context.Message)
        {
            case Started:
            {
                var identity = context.ClusterIdentity()!.Identity;
                var starts = Starts.AddOrUpdate(identity, 1, (_, count) => count + 1);

                // Simulates loading the state from a slow database
                await Task.Delay(StartDuration);

                if (identity.StartsWith(AlwaysFail) || (identity.StartsWith(FailFirstStart) && starts == 1))
                {
                    throw new InvalidOperationException("Could not load the state");
                }

                _state = "loaded";

                break;
            }
            case Stopping or Stopped when context.ClusterIdentity()!.Identity.StartsWith(AlwaysFailWithStopHandlers):
            {
                StopHandlerCalls.AddOrUpdate(context.ClusterIdentity()!.Identity, 1, (_, count) => count + 1);

                // Like a generated grain, whose inner grain does not exist when its creation failed
                _ = _state!.Length;

                break;
            }
            case Ping ping:
                context.Respond(new Pong { Message = ping.Message });

                break;
        }
    }
}
