### Proto.Cluster.Tests
`System.Exception: Failed to reach consensus` observed during cluster initialization in multiple tests (e.g., RedundantGossipTests.GossipRequest_is_sent_multiple_times_without_state_changes).

### Proto.Cluster.Tests (NullReference)
`System.NullReferenceException: Object reference not set to an instance of an object.` at `Gossiper.StartGossipActorAsync` during multiple tests in `Proto.Cluster.Tests`.

### Proto.Cluster.Tests (Timeout)
`System.TimeoutException: Request timed out` in `InMemoryPartitionActivatorClusterTests.HandlesSlowResponsesCorrectly`.

### Proto.Actor.Tests.EscalateFailureTests
`System.TimeoutException: The condition was not met within the timeout of 00:00:00.1000000` in `Proto.Mailbox.Tests.EscalateFailureTests.GivenNonCompletedSystemMessageTaskThrewException_ShouldEscalateFailure`.

### Proto.Mailbox.Tests.EscalateFailureTests.GivenNonCompletedUserMessageTaskGotCancelled_ShouldEscalateFailure
`System.TimeoutException: The condition was not met within the timeout of 00:00:00.1000000` when waiting for mailbox failure escalation.

### Proto.Tests.SupervisionTestsAllForOne.AllForOneStrategy_Should_PassExceptionOnRestart
`System.InvalidOperationException: Collection was modified; enumeration operation may not execute.` during enumeration of mailbox statistics.

### Proto.Cluster.Tests.GossipCoreTests.Large_cluster_should_get_topology_consensus
`Test failure: output indicated [FAIL] but passed on rerun; no stack trace captured.`

### Proto.Tests.SupervisionTestsAlwaysRestart.AlwaysRestartStrategy_Should_RestartFailingChildOnly
`Assert.Equal() Failure: Values differ. Expected: 1 Actual: 0` in `SupervisionTests_AlwaysRestart.cs:line 43`.

### Proto.Cluster.Tests.ClusterTopologyBuilderTests.Compute_FiltersBlockedAndDuplicates
`Assert.Equal() Failure: Strings differ Expected: "4" Actual: "3"`

### Proto.Cluster.Tests.GossipCoreTests.Large_cluster_should_get_topology_consensus
`Expected x.consensus to be True, but found False.`

### Proto.Tests.SupervisionTestsOneForOne.OneForOneStrategy_Should_EscalateFailureToParent
`System.InvalidOperationException: Sequence contains no elements` in `SupervisionTests_OneForOne.cs:line 263`.
### Proto.Mailbox.Tests.MailboxSchedulingTests.GivenNonCompletedUserMessage_ShouldHaltProcessingUntilCompletion
`System.TimeoutException: The condition was not met within the timeout of 00:00:00.1000000`

### Proto.Tests.ActorTests.StopActorWithLongRunningTask
`Proto.TestKit.TestKitException: Expected user message of type System.Threading.Tasks.TaskCanceledException, but received system message of type Proto.Stopping`

### Proto.Tests.ReceiveTimeoutTests.receive_timeout_is_reset_by_influencing_messages
`Proto.TestKit.TestKitException : Waited 1 seconds but failed to receive a message` observed when ReceiveTimeout was not delivered after cancelling scheduled ticks.

### Proto.Cluster.Tests.RedundantGossipTests.GossipRequest_is_sent_multiple_times_without_state_changes
`Expected fixture.SerializedKeyCount to be 3, but found 1.`

### Proto.Cluster.Tests.GossipCoreTests.Large_cluster_should_get_topology_consensus
`Expected x.consensus to be True, but found False.`

### EndpointReader.RunReader
`System.InvalidOperationException: Can't read messages after the request is complete.` observed when a node leaves the cluster.

### Proto.Tests.ReceiveTimeoutTests.receive_timeout_received_within_expected_time_when_sending_ignored_messages
```
Proto.TestKit.TestKitException : Waited 1 seconds but failed to receive a message
```

### Proto.Tests.SupervisionTestsAlwaysRestart.AlwaysRestartStrategy_Should_RestartFailingChildOnly
`Assert.Equal() Failure: Values differ. Expected: 1 Actual: 0` encountered during initial `dotnet test` run; rerun passed.

### Proto.Cluster.Tests.GossipCoreTests.Large_cluster_should_get_topology_consensus
`Expected x.consensus to be True, but found False.` occurred twice before extending the consensus timeout to 20 seconds.

### Proto.Tests.SharedFutureTests.Should_wrap_request_ids_without_hitting_zero
`Expected future.Pid.RequestId to be 8u, but found 1u.` observed when verifying request id wrap-around on .NET 8 after the upgrade; focused rerun and a full-suite retry passed.

### Issue2151 KubernetesProvider.DeregisterMemberInner
```
System.ArgumentNullException: Value cannot be null. (Parameter 'source')
   at System.Linq.ThrowHelper.ThrowArgumentNullException(ExceptionArgument argument)
   at System.Linq.Enumerable.Where[TSource](IEnumerable`1 source, Func`2 predicate)
   at Proto.Cluster.Kubernetes.KubernetesProvider.DeregisterMemberInner(Cluster cluster)
```

### Proto.Cluster.PartitionIdentity.Tests.PartitionIdentityTests.ClusterMaintainsSingleConcurrentVirtualActorPerIdentity(identityCount: 100, batchSize: 5, threads: 12, runtimeSeconds: 20, mode: Pull, send: Full)
```
Consistency error, actual: 1681, expected: 1663, stored: 1680, global: 1681
```
Failed on CI (net10.0) after the .NET 10-only migration: two activations of the same identity overlapped while members were joining/leaving (Pull/Full rebalance).
Not a regression: pinned to 2 cores (`taskset -c 0,1`, like the 2-vCPU CI runners) it failed 1/6 runs on the upstream commit 6a570628 and 2/6 on the migrated code; with 16 cores both passed 5/5. Flaky under CPU starvation.
Root cause found: when an activation request to the activator timed out, the identity owner reported a failure and the retry picked another activator (round-robin), while the timed-out request was still processed by the first one, creating a second activation. Fixed by retrying the same activator (`PartitionConfig.ActivationRequestAttempts`) and remembering unresponsive activators; reproduced by `SlowActivatorTests` (3 activations before the fix, 1 after). After the fix the chaos test passed 6/6 runs pinned to 2 cores.

### Proto.Cluster.MongoIdentity.Tests.ChaosMongoIdentityClusterFixture (class fixture initialization)
```
System.Exception : Failed to reach consensus
   at Proto.Cluster.Tests.ClusterFixture.SpawnClusterNodes(Int32 count, Func`2 configure) in tests/Proto.Cluster.Tests/ClusterFixture.cs:line 323
```
All 23 tests of the fixture failed in 1 ms because the 3-member test cluster did not reach gossip topology consensus while starting (first run right after a build, many fixtures starting in parallel). Unrelated to MongoDB; the next two runs passed 70/70.

**Update:** this explanation was wrong. The real cause is the `TestProvider` stale snapshot race described in the "MongoDB 9.0.2" entry below.

### Proto.Cluster.MongoIdentity.Tests.ChaosMongoIdentityClusterFixture (class fixture initialization, CI)
```
System.Exception : Failed to reach consensus
   at Proto.Cluster.Tests.ClusterFixture.SpawnClusterNodes(Int32 count, Func`2 configure) in tests/Proto.Cluster.Tests/ClusterFixture.cs:line 322
   at Proto.Cluster.Tests.ClusterFixture.InitializeAsync() in tests/Proto.Cluster.Tests/ClusterFixture.cs:line 126
```
Same failure on the GitHub Actions runner (2 vCPU): all 23 tests of the chaos fixture failed, 47 others passed.

**Update:** this explanation was wrong. The real cause is the `TestProvider` stale snapshot race described in the "MongoDB 9.0.2" entry below.

### Proto.Cluster.MongoIdentity.Tests.ChaosMongoIdentityClusterFixture (class fixture initialization, CI, MongoDB 9.0.2)
```
System.Exception : Failed to reach consensus
   at Proto.Cluster.Tests.ClusterFixture.SpawnClusterNodes(Int32 count, Func`2 configure) in tests/Proto.Cluster.Tests/ClusterFixture.cs:line 322
   at Proto.Cluster.Tests.ClusterFixture.InitializeAsync() in tests/Proto.Cluster.Tests/ClusterFixture.cs:line 126
```
Root cause found (the earlier "unrelated, flaky under load" notes above were wrong): a race in `TestProvider.NotifyStatuses`. The in-memory agent raises status updates on the thread of whichever member registers, and the provider read the member snapshot and applied it without mutual exclusion. A thread holding an older snapshot (2 members) could apply it after another thread applied the newer one (3 members), so the member list saw a joined member as left and blocked it permanently (`I have been blocked, exiting`), and consensus was never reached. Both Mongo cluster fixtures were affected, also when run alone. Fixed by serializing snapshot read and apply per provider. Pinned to 2 cores against MongoDB 9: before the fix 4/8 runs failed, each failing run logged a blocked member; after the fix 12/12 passed with no blocked member. Test infrastructure only; upstream had the same code but never ran the Mongo tests in CI.

Follow-up from code review: stopped `TestProvider`s now unsubscribe from the shared `InMemAgent` (before, dead members kept receiving and applying snapshots), and `InMemAgent` delivers each status update to every handler even if one throws (before, one failing handler aborted delivery and made `RegisterService` throw for the joining member). Both covered by `TestProviderTests`, which failed before the change.
