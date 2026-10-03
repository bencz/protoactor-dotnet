# Proto.Actor (.NET), bencz fork

[![Build and test](https://github.com/bencz/protoactor-dotnet/actions/workflows/build-dev.yml/badge.svg?branch=dev)](https://github.com/bencz/protoactor-dotnet/actions/workflows/build-dev.yml)

Ultra-fast, distributed, cross-platform actors.

This is a fork of [asynkron/protoactor-dotnet](https://github.com/asynkron/protoactor-dotnet), maintained at
[https://github.com/bencz/protoactor-dotnet](https://github.com/bencz/protoactor-dotnet). It focuses on running virtual
actors (grains) in scalable environments such as Kubernetes with autoscaling.

## Differences from upstream

- **.NET 10 only**: all libraries, tests, examples and benchmarks target `net10.0`.
- **PostgreSQL providers**: `Proto.Cluster.Identity.PostgreSql` (identity lookup storage) and
  `Proto.Cluster.SeedNode.PostgreSql` (seed node discovery with expiring entries).
- **Safer activations**
  - An activation request that times out is retried against the same activator instead of another member, so a slow
    member no longer leads to duplicate activations.
  - A virtual actor whose `Started` handler fails is deactivated instead of restarted in a loop; the next request
    activates it again.
- **Identity storage cleanup**: leftovers of members that are gone (e.g. after the whole cluster was restarted) are
  removed by a single member; Redis keeps a member registry instead of scanning the keyspace.
- **Redis and MongoDB fixes**
  - Redis seed entries expire unless refreshed (requires Redis/Valkey 7.4+), and removing a member no longer leaves
    keys behind.
  - MongoDB seed members can register concurrently, and the stale lock wait is configurable.
- **Removed**: Couchbase and DynamoDB persistence, Amazon ECS and Azure Container Apps cluster providers.
- **No NuGet packages**: this fork is not published to NuGet (see [Installing](#installing)).

See the [Virtual Actors Guide](VIRTUAL_ACTORS.md) and [Virtual actors in scalable environments](#virtual-actors-in-scalable-environments) for configuration guidance.

## Installing

The `Proto.*` packages on NuGet are the upstream releases, not this fork. To use the fork, reference its projects
from source, for example as a git submodule:

```bash
git submodule add https://github.com/bencz/protoactor-dotnet.git external/protoactor-dotnet
```

```xml
<ItemGroup>
  <ProjectReference Include="external/protoactor-dotnet/src/Proto.Actor/Proto.Actor.csproj" />
  <ProjectReference Include="external/protoactor-dotnet/src/Proto.Cluster/Proto.Cluster.csproj" />
</ItemGroup>
```

Requires the .NET 10 SDK.

## Source code

This is the .NET implementation of Proto Actor.

Other implementations:

- Go: [https://github.com/asynkron/protoactor-go](https://github.com/asynkron/protoactor-go)

## Documentation

Additional root-level documents provide deeper insights into the project:

- [VIRTUAL_ACTORS.md](VIRTUAL_ACTORS.md): complete guide to building, configuring and operating virtual actors (grains).
- [CODEBASE_OVERVIEW.md](CODEBASE_OVERVIEW.md): overview of the repository structure and key concepts.
- [CLUSTER_MEMBERSHIP_GOSSIP.md](CLUSTER_MEMBERSHIP_GOSSIP.md): explains how cluster membership is detected and propagated via gossip.
- [EVENTSTREAM_EVENTS.md](EVENTSTREAM_EVENTS.md): lists key EventStream events and their publishers/subscribers.
- [SECURITY.md](SECURITY.md): security policy and supported versions.
- [Terminology.md](Terminology.md): definitions of common Proto.Actor terms.

The upstream [Proto.Actor documentation](https://proto.actor/docs/) also applies to this fork.

## Test coverage

You can capture code coverage directly when running the test projects. The .NET SDK ships an `XPlat Code Coverage` data collector that works across platforms, so you do not need any third-party tooling for the basic workflow.

### Quick coverage run

Run any test project with coverage enabled by passing the `CollectCoverage` property:

```bash
dotnet test /p:CollectCoverage=true
```

This uses the built-in collector and writes coverage data under the `TestResults` directory for the invocation.

### Using coverlet.collector

For richer reporting support you can opt into the `coverlet.collector` package. Add it to the test project you want to analyze:

```bash
dotnet add package coverlet.collector
```

Then request coverage when running the tests:

```bash
dotnet test --collect:"XPlat Code Coverage"
```

This produces a `.coverage` file under `TestResults/...` that can be converted into other formats.

### Alternative output formats

You can ask the collector to emit other formats such as Cobertura, lcov, or OpenCover. For example, to produce an `lcov` report:

```bash
dotnet test --collect:"XPlat Code Coverage" -- DataCollectionRunSettings.DataCollectors.DataCollector.Configuration.Format=lcov
```

### HTML reports

Feed the generated `lcov` or `cobertura` files into tools like [ReportGenerator](https://github.com/danielpalme/ReportGenerator) to build browsable HTML output. Install the global tool and point it at your coverage reports:

```bash
dotnet tool install -g dotnet-reportgenerator-globaltool
reportgenerator -reports:**/coverage.cobertura.xml -targetdir:coveragereport
```

Open the files in the `coveragereport` directory to inspect the results.

## Design principles

**Minimalistic API** - The API should be small and easy to use. Avoid enterprisey containers and configurations.

**Build on existing technologies** - There are already a lot of great technologies for e.g. networking and clustering. Build on those instead of reinventing them. E.g. gRPC streams for networking, Consul for clustering.

**Pass data, not objects** - Serialization is an explicit concern - don't try to hide it. Protobuf all the way.

**Be fast** - Do not trade performance for magic API trickery.

## Getting started

The best place for learning how to use Proto.Actor is the [examples](https://github.com/bencz/protoactor-dotnet/tree/dev/examples), together with the upstream [documentation](https://proto.actor/docs/).

### Hello world

Define a message type:

```csharp
internal record Hello(string Who);
```

Define an actor:

```csharp
internal class HelloActor : IActor
{
    public Task ReceiveAsync(IContext context)
    {
        var msg = context.Message;
        if (msg is Hello r)
        {
            Console.WriteLine($"Hello {r.Who}");
        }
        return Task.CompletedTask;
    }
}
```

Spawn it and send a message to it:

```csharp
var system = new ActorSystem();
var context = system.Root;
var props = Props.FromProducer(() => new HelloActor());
var pid = context.Spawn(props);

context.Send(pid, new Hello("Alex"));
```

You should see the output `Hello Alex`.

## Virtual actors in scalable environments

Guidance for running virtual actors (grains) on Kubernetes with autoscaling (HPA), especially when a grain loads its
state from a database in `Started`.

This section is a summary; the [Virtual Actors Guide](VIRTUAL_ACTORS.md) covers grain design, configuration, hosting,
Pub/Sub, testing and a review checklist in depth.

### How an activation works

- The first request for an identity spawns the grain and returns its PID right away; the activation does not wait for
  `Started`.
- `Started` always runs before any request reaches the grain. Requests sent meanwhile wait in its mailbox.
- A slow `Started` therefore makes requests slower, never activations. Size `ActorRequestTimeout` for it, not
  `ActorActivationTimeout`.
- If `Started` throws, the grain is deactivated: pending and incoming requests get a `DeadLetterResponse`, callers
  retry, and the next request activates it again. Nothing restarts in the background.

### Choosing an identity lookup

| Lookup | Where placements live | Use it when |
|---|---|---|
| `PartitionIdentityLookup` (default) | In memory, distributed over the members | Kubernetes and HPA. No database needed, fastest lookups. Recommended. |
| `IdentityStorageLookup` with `RedisIdentityStorage`, `MongoIdentityStorage` or `PostgreSqlIdentityStorage` | In a database | Placements must outlive the members, or the database should be the single source of truth during topology changes. Adds a database round trip to every lookup that misses the PID cache. |
| `PartitionActivatorLookup` | In memory, the owner is also the activator | Avoid with HPA and a slow `Started`: every scale event moves grains, and the old and new activation briefly run side by side. |

### Recommended configuration (Partition lookup, HPA, slow `Started`)

```csharp
var clusterConfig = ClusterConfig
    .Setup(clusterName, clusterProvider, new PartitionIdentityLookup(new PartitionConfig
    {
        // Longer than a rebalance (RebalanceActivationsCompletionTimeout plus the handover pull),
        // so requests survive scale events instead of timing out
        GetPidTimeout = TimeSpan.FromSeconds(15),

        // At least ActivationRequestAttempts x ActorActivationTimeout,
        // so a rebalance waits for activation requests that are still being retried
        RebalanceActivationsCompletionTimeout = TimeSpan.FromSeconds(15),

        // A timed out activation request is retried on the same member, which avoids duplicate activations
        ActivationRequestAttempts = 2
    }))
    // Not affected by Started, which runs after the PID is returned
    .WithActorActivationTimeout(TimeSpan.FromSeconds(5))
    // Longer than the slowest Started plus the request handler; whole seconds, minimum 1 s
    .WithActorRequestTimeout(TimeSpan.FromSeconds(25))
    .WithClusterKind("user", Props.FromProducer(() => new UserGrain())
        .WithClusterRequestDeduplication()
        .WithStartDeadline(TimeSpan.FromSeconds(20)));
```

### Writing grains

- **Load state with `await` in `Started`.** Do not use `ReenterAfter` there: the handler would return early and requests
  would run before the state is loaded.
- **Pass `context.CancellationToken` to database calls.** It is cancelled when the grain is stopped, for example on
  scale-in, so a long load does not hold the shutdown.
- **Expect requests to be delivered more than once.**
  - The cluster resends a request after every `ActorRequestTimeout` until the caller's cancellation token expires.
  - Make handlers idempotent, and add `.WithClusterRequestDeduplication()` to the grain props. Its window (default
    1 minute) must be longer than the callers' total timeout.
- **Use optimistic concurrency for persisted state** (a version or etag checked on write). No lookup can rule out two
  activations of the same identity in every failure scenario, for example a network partition. A version check makes
  the second writer fail instead of overwriting data.
- **Give callers a cancellation token longer than the slowest `Started`**, e.g. 20 to 30 seconds.

### Kubernetes

- **`terminationGracePeriodSeconds`**
  - On shutdown every grain is stopped and finishes the messages already queued for it.
  - Grains are stopped 20 at a time, waiting up to 10 seconds per group, so allow at least
    `ceil(grains per pod / 20) x 10` seconds, plus a few seconds to leave the cluster.
- **HPA scale-down stabilization window** (`behavior.scaleDown.stabilizationWindowSeconds`). Fewer topology changes
  mean fewer rebalances and fewer requests waiting for one.
- **`ClusterConfig.WithHeartbeatExpiration(TimeSpan.FromSeconds(20))`.** Heartbeat expiration is disabled by default;
  enable it to block members that stop responding without leaving, instead of waiting for the cluster provider to
  notice.

### Identity storage settings

When using `IdentityStorageLookup`:

- **Lock wait.** A member that finds an identity locked waits `maxWaitBeforeStaleLock` (default 5 seconds) before
  treating the lock as abandoned. Keep it above `ActorActivationTimeout` plus the database latency under load, or locks
  that are still in use get removed and the identity is activated twice. Configure it with
  `new RedisIdentityStorage(clusterName, multiplexer, maxWaitBeforeStaleLock: ...)`,
  `new MongoIdentityStorage(clusterName, collection, maxWaitBeforeStaleLock: ...)` or
  `new PostgreSqlIdentityStorageOptions { MaxWaitBeforeStaleLock = ... }`.
- **Cleanup.** When members leave, or after the whole cluster restarts, one member removes the placements owned by
  members that are gone. Activations never expire on their own, so long-lived grains are safe.
- **PostgreSQL schema.** The table and its indexes are created on startup. Set `CreateSchema = false` and apply
  `PostgreSqlIdentityStorage.CreateSchemaSql()` through your migrations if the schema is managed separately.

### Seed node discovery

`SeedNodeClusterProvider.JoinWithDiscovery(...)` finds the first members to join through a shared store, as an
alternative to the Kubernetes or Consul providers. After joining, membership is kept up to date by gossip.

- **PostgreSQL** (`PostgreSqlSeedNodeDiscovery`, package `Proto.Cluster.SeedNode.PostgreSql`)
  - Entries expire after `MemberTtl` (default 30 seconds) unless their member keeps refreshing them, so pods that die
    without deregistering disappear on their own.
  - Expiration uses the database clock, so clock differences between pods do not matter.
  - Several clusters can share the table; entries are separated by cluster name.
  - The table is created on first use; set `CreateSchema = false` and apply
    `PostgreSqlSeedNodeDiscovery.CreateSchemaSql()` through your migrations if the schema is managed separately.
- **Redis** (`RedisSeedNodeDiscovery`, package `Proto.Cluster.SeedNode.Redis`)
  - Entries expire after `memberTtl` (default 30 seconds) unless refreshed, like PostgreSQL.
  - Requires Redis or Valkey 7.4 or later (hash field expiration).
- **MongoDB** (`MongoDbSeedNodeDiscovery`, package `Proto.Cluster.SeedNode.MongoDb`)
  - Entries do not expire; they are removed on shutdown or when another member fails to connect to them.

```csharp
var dataSource = NpgsqlDataSource.Create(connectionString);

var clusterProvider = SeedNodeClusterProvider.JoinWithDiscovery(
    new PostgreSqlSeedNodeDiscovery(dataSource, clusterName));
```

## Acknowledgements

Proto.Actor was created by [Asynkron AB](https://asynkron.se) and its contributors; this fork builds on their work and
keeps the original [Apache 2.0 license](LICENSE). Upstream partners, sponsors and contributor companies:

| Name                                     | Role                                  |
| ---------------------------------------- | ------------------------------------- |
| [Asynkron AB](https://asynkron.se)       | Founder and owner of Proto.Actor      |
| Helleborg AS                             | Core contributor team                 |
| [Ubiquitous AS](https://ubiquitous.no/)  | Core contributor team                 |
| [Ahoy Games](https://www.ahoygames.com/) | Core contributor team                 |
| [Etteplan](https://www.etteplan.com/)    | Contributing tutorials, documentation |
