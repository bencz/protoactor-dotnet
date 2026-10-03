// -----------------------------------------------------------------------
// <copyright file="PartitionConfig.cs" company="Asynkron AB">
//      Copyright (C) 2015-2025 Asynkron AB All rights reserved
// </copyright>
// -----------------------------------------------------------------------

using System;

namespace Proto.Cluster.Partition;

public record PartitionConfig
{
    public TimeSpan GetPidTimeout { get; init; } = TimeSpan.FromSeconds(5);
    public int HandoverChunkSize { get; init; } = 5000;

    /// <summary>
    ///     This is the longest the system will wait for the current activations to complete before forcing a rebalance.
    /// </summary>
    public TimeSpan RebalanceActivationsCompletionTimeout { get; init; } = TimeSpan.FromSeconds(10);

    public TimeSpan RebalanceRequestTimeout { get; init; } = TimeSpan.FromSeconds(2);

    /// <summary>
    ///     How many times the identity owner sends an activation request to the same activator when the activator does not
    ///     answer within <see cref="ClusterConfig.ActorActivationTimeout" />. A request that timed out may still be
    ///     processed by a busy activator, so it is retried against that same activator (which returns the activation it
    ///     created) instead of another member, where it would create a second activation.
    ///     Keep <see cref="RebalanceActivationsCompletionTimeout" /> at least as long as
    ///     <c>ActivationRequestAttempts * ActorActivationTimeout</c>, so a rebalance waits for these retries.
    /// </summary>
    public int ActivationRequestAttempts { get; init; } = 2;

    /// <summary>
    ///     Determines which side initiates the identity handover.
    ///     Pull is stable, Push is currently experimental
    /// </summary>
    public PartitionIdentityLookup.Mode Mode { get; init; } = PartitionIdentityLookup.Mode.Pull;

    /// <summary>
    ///     Determines if all activations are sent or only ones that changed owner from the previous topology
    ///     Delta is currently experimental.
    /// </summary>
    public PartitionIdentityLookup.Send Send { get; init; } = PartitionIdentityLookup.Send.Full;

    public bool DeveloperLogging { get; init; } = false;
}