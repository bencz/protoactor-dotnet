using System;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using Proto;
using Proto.TestKit;
using Xunit;

namespace Proto.TestKit.Tests
{
public class TestProbeAsyncTests
{
    [Fact]
    public async Task GetNextUserMessageAsync_returns_message()
    {
        var system = new ActorSystem();
        var (probe, pid) = system.CreateTestProbe();

        system.Root.Send(pid, "hello");
        var message = await probe.GetNextUserMessageAsync<string>(s => s == "hello");
        message.Should().Be("hello");
    }

    [Fact]
    public async Task Default_receive_timeout_tolerates_a_message_that_arrives_after_one_second()
    {
        var system = new ActorSystem();
        var (probe, pid) = system.CreateTestProbe();

        // Simulates a slow CI runner: the expected message arrives later than the old one second default
        _ = Task.Delay(TimeSpan.FromMilliseconds(1500)).ContinueWith(_ => system.Root.Send(pid, "late"));

        await probe.ExpectNextUserMessageAsync<string>(s => s == "late");
    }

    [Fact]
    public async Task Default_no_message_timeout_stays_short()
    {
        var system = new ActorSystem();
        var (probe, _) = system.CreateTestProbe();
        var started = DateTime.UtcNow;

        await probe.ExpectNoMessageAsync();

        (DateTime.UtcNow - started).Should().BeLessThan(TimeSpan.FromSeconds(3));
    }

    [Fact]
    public async Task GetNextMessageAsync_with_predicate_returns_specific()
    {
        var system = new ActorSystem();
        var (probe, pid) = system.CreateTestProbe();

        system.Root.Send(pid, "a");
        system.Root.Send(pid, "b");
        var msg = await probe.FishForMessageAsync<string>(x => x == "b");
        msg.Should().Be("b");
    }

    [Fact]
    public async Task ExpectEmptyMailboxAsync_detects_empty_mailbox()
    {
        var system = new ActorSystem();
        var (probe, pid) = system.CreateTestProbe();

        system.Root.Send(pid, "init");
        await probe.ExpectNextUserMessageAsync<string>(s => s == "init");

        await probe.ExpectEmptyMailboxAsync();
    }

    [Fact]
    public async Task ExpectEmptyMailboxAsync_throws_when_message_present()
    {
        var system = new ActorSystem();
        var (probe, pid) = system.CreateTestProbe();

        system.Root.Send(pid, "hello");
        await Assert.ThrowsAsync<TestKitException>(() => probe.ExpectEmptyMailboxAsync());
    }

    [Fact]
    public async Task SubsequentStartedMessages_are_enqueued()
    {
        var system = new ActorSystem();
        var (probe, pid) = system.CreateTestProbe();

        // Send an extra Started message and ensure probe receives it
        system.Root.Send(pid, Started.Instance);

        await probe.ExpectNextSystemMessageAsync<Started>();
    }
}
}
