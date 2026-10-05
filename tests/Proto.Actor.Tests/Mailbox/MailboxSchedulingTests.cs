using System;
using System.Threading.Tasks;
using Proto.TestFixtures;
using Proto.TestKit;
using static Proto.TestKit.TestKit;
using Xunit;

namespace Proto.Mailbox.Tests;

public class MailboxSchedulingTests
{
    // Waits end as soon as the condition holds; the margin only matters on slow CI runners
    private static readonly TimeSpan ConditionTimeout = TimeSpan.FromSeconds(5);

    [Fact]
    public async Task GivenNonCompletedUserMessage_ShouldHaltProcessingUntilCompletion()
    {
        var mailboxHandler = new TestMailboxHandler();
        var userMailbox = new UnboundedMailboxQueue();
        var systemMessages = new UnboundedMailboxQueue();
        var mailbox = new DefaultMailbox(systemMessages, userMailbox);
        mailbox.RegisterHandlers(mailboxHandler, mailboxHandler);

        var msg1 = new TestMessageWithTaskCompletionSource();
        var msg2 = new TestMessageWithTaskCompletionSource();

        mailbox.PostUserMessage(msg1);
        mailbox.PostUserMessage(msg2);

        await AwaitConditionAsync(() => userMailbox.HasMessages, ConditionTimeout);

        Assert.True(userMailbox.HasMessages,
            "Mailbox should not have processed msg2 because processing of msg1 is not completed."
        );

        msg2.TaskCompletionSource.SetResult(0);
        msg1.TaskCompletionSource.SetResult(0);
        await AwaitConditionAsync(() => !userMailbox.HasMessages, ConditionTimeout);

        Assert.False(userMailbox.HasMessages,
            "Mailbox should have processed msg2 because processing of msg1 is completed."
        );
    }

    [Fact]
    public async Task GivenCompletedUserMessage_ShouldContinueProcessing()
    {
        var mailboxHandler = new TestMailboxHandler();
        var userMailbox = new UnboundedMailboxQueue();
        var systemMessages = new UnboundedMailboxQueue();
        var mailbox = new DefaultMailbox(systemMessages, userMailbox);
        mailbox.RegisterHandlers(mailboxHandler, mailboxHandler);

        var msg1 = new TestMessageWithTaskCompletionSource();
        var msg2 = new TestMessageWithTaskCompletionSource();
        msg1.TaskCompletionSource.SetResult(0);
        msg2.TaskCompletionSource.SetResult(0);

        mailbox.PostUserMessage(msg1);
        mailbox.PostUserMessage(msg2);

        await AwaitConditionAsync(() => !userMailbox.HasMessages, ConditionTimeout);

        Assert.False(userMailbox.HasMessages,
            "Mailbox should have processed both messages because they were already completed."
        );
    }

    [Fact]
    public async Task GivenNonCompletedSystemMessage_ShouldHaltProcessingUntilCompletion()
    {
        var mailboxHandler = new TestMailboxHandler();
        var userMailbox = new UnboundedMailboxQueue();
        var systemMessages = new UnboundedMailboxQueue();
        var mailbox = new DefaultMailbox(systemMessages, userMailbox);
        mailbox.RegisterHandlers(mailboxHandler, mailboxHandler);

        var msg1 = new TestMessageWithTaskCompletionSource();
        var msg2 = new TestMessageWithTaskCompletionSource();

        mailbox.PostSystemMessage(msg1);
        mailbox.PostSystemMessage(msg2);

        Assert.True(systemMessages.HasMessages,
            "Mailbox should not have processed msg2 because processing of msg1 is not completed."
        );

        msg2.TaskCompletionSource.SetResult(0);
        msg1.TaskCompletionSource.SetResult(0);
        await AwaitConditionAsync(() => !systemMessages.HasMessages, ConditionTimeout);

        Assert.False(systemMessages.HasMessages,
            "Mailbox should have processed msg2 because processing of msg1 is completed."
        );
    }

    [Fact]
    public async Task GivenCompletedSystemMessage_ShouldContinueProcessing()
    {
        var mailboxHandler = new TestMailboxHandler();
        var userMailbox = new UnboundedMailboxQueue();
        var systemMessages = new UnboundedMailboxQueue();
        var mailbox = new DefaultMailbox(systemMessages, userMailbox);
        mailbox.RegisterHandlers(mailboxHandler, mailboxHandler);

        var msg1 = new TestMessageWithTaskCompletionSource();
        var msg2 = new TestMessageWithTaskCompletionSource();
        msg1.TaskCompletionSource.SetResult(0);
        msg2.TaskCompletionSource.SetResult(0);

        mailbox.PostSystemMessage(msg1);
        mailbox.PostSystemMessage(msg2);
        await AwaitConditionAsync(() => !systemMessages.HasMessages, ConditionTimeout);

        Assert.False(systemMessages.HasMessages,
            "Mailbox should have processed both messages because they were already completed."
        );
    }

    [Fact]
    public async Task GivenNonCompletedUserMessage_ShouldSetMailboxToIdleAfterCompletion()
    {
        var mailboxHandler = new TestMailboxHandler();
        var userMailbox = new UnboundedMailboxQueue();
        var systemMessages = new UnboundedMailboxQueue();
        var mailbox = new DefaultMailbox(systemMessages, userMailbox);
        mailbox.RegisterHandlers(mailboxHandler, mailboxHandler);

        var msg1 = new TestMessageWithTaskCompletionSource();
        mailbox.PostUserMessage(msg1);

        await AwaitConditionAsync(() => !userMailbox.HasMessages, ConditionTimeout);
        msg1.TaskCompletionSource.SetResult(0);
        await AwaitConditionAsync(() => mailbox.Status == 0, ConditionTimeout);

        // Mailbox becomes idle (status 0) after completing the user message
        Assert.Equal(0, mailbox.Status);
    }
}
