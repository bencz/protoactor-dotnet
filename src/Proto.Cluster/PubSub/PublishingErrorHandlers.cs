using System;
using System.Threading.Tasks;

namespace Proto.Cluster.PubSub;

/// <summary>
///     Ready-made <see cref="PublishingErrorHandler" /> policies for <see cref="BatchingProducerConfig.OnPublishingError" />.
/// </summary>
public static class PublishingErrorHandlers
{
    /// <summary>
    ///     Default policy. Retries a failed batch with exponential backoff, then fails only that batch and keeps the producer
    ///     running. Transient errors, such as a publish timing out while the cluster rebalances, do not stop the producer.
    /// </summary>
    /// <param name="maxRetries">Retries after the first attempt before the batch is failed. Default is 3.</param>
    /// <param name="initialBackoff">Delay before the first retry, doubled on every retry. Default is 100 ms.</param>
    public static PublishingErrorHandler RetryThenFailBatch(int maxRetries = 3, TimeSpan? initialBackoff = null)
    {
        var backoff = initialBackoff ?? TimeSpan.FromMilliseconds(100);

        return (retries, _, _) => Task.FromResult(
            retries <= maxRetries
                ? PublishingErrorDecision.RetryBatchAfter(backoff * Math.Pow(2, retries - 1))
                : PublishingErrorDecision.FailBatchAndContinue
        );
    }

    /// <summary>
    ///     Fails the batch and stops the producer on the first error; every later publish fails as well.
    /// </summary>
    public static readonly PublishingErrorHandler FailBatchAndStop =
        (_, _, _) => Task.FromResult(PublishingErrorDecision.FailBatchAndStop);
}
