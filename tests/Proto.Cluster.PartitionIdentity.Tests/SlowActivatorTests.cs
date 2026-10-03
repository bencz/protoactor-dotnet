#nullable enable
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using ClusterTest.Messages;
using FluentAssertions;
using Proto.Cluster.Identity;
using Proto.Cluster.Partition;
using Proto.Cluster.Tests;
using Xunit;

namespace Proto.Cluster.PartitionIdentity.Tests;

/// <summary>
///     An activator that answers an activation request after the requester stopped waiting must not lead to a second
///     activation on another member.
/// </summary>
public class SlowActivatorTests : IClassFixture<SlowActivatorClusterFixture>
{
    private readonly SlowActivatorClusterFixture _fixture;

    public SlowActivatorTests(SlowActivatorClusterFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task TimedOutActivationRequestDoesNotCreateASecondActivation()
    {
        var identity = $"slow-{Guid.NewGuid():N}";

        var pong = await _fixture.Members[0].RequestAsync<Pong>(identity, StartCountingActor.Kind,
            new Ping { Message = "hello" }, new CancellationTokenSource(TimeSpan.FromSeconds(20)).Token);

        pong.Should().NotBeNull();

        // Delayed activation requests still in the activators' mailboxes are processed by now
        await Task.Delay(SlowActivatorClusterFixture.ActivatorDelay * 2);

        StartCountingActor.Starts.GetValueOrDefault(identity).Should().Be(1);
    }
}

public class SlowActivatorClusterFixture : BaseInMemoryClusterFixture
{
    public static readonly TimeSpan ActivationTimeout = TimeSpan.FromSeconds(1);

    // Longer than the activation timeout, so the identity owner stops waiting before the activator answers
    public static readonly TimeSpan ActivatorDelay = TimeSpan.FromSeconds(1.5);

    public SlowActivatorClusterFixture() : base(3, config => config.WithActorActivationTimeout(ActivationTimeout))
    {
    }

    protected override ClusterKind[] ClusterKinds =>
        new[] { new ClusterKind(StartCountingActor.Kind, Props.FromProducer(() => new StartCountingActor())) };

    protected override IIdentityLookup GetIdentityLookup(string clusterName)
    {
        var delayedIdentities = new ConcurrentDictionary<ClusterIdentity, bool>();

        return new PartitionIdentityLookup(new PartitionConfig(), props => props.WithReceiverMiddleware(next =>
            async (context, envelope) =>
            {
                // Simulates a busy activator: the first activation request for an identity on this member is held
                // longer than the activation timeout before it is processed
                if (envelope.Message is ActivationRequest request && delayedIdentities.TryAdd(request.ClusterIdentity, true))
                {
                    await Task.Delay(ActivatorDelay);
                }

                await next(context, envelope);
            }));
    }
}

public class StartCountingActor : IActor
{
    public const string Kind = "start-counting";

    public static readonly ConcurrentDictionary<string, int> Starts = new();

    public Task ReceiveAsync(IContext context)
    {
        switch (context.Message)
        {
            case Started:
                Starts.AddOrUpdate(context.ClusterIdentity()!.Identity, 1, (_, count) => count + 1);

                break;
            case Ping ping:
                context.Respond(new Pong { Message = ping.Message });

                break;
        }

        return Task.CompletedTask;
    }
}
