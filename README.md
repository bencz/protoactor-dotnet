# Proto.Actor (.NET) — bencz fork

[![Build and test](https://github.com/bencz/protoactor-dotnet/actions/workflows/build-dev.yml/badge.svg?branch=dev)](https://github.com/bencz/protoactor-dotnet/actions/workflows/build-dev.yml)

Ultra-fast, distributed, cross-platform actors.

This is a fork of [asynkron/protoactor-dotnet](https://github.com/asynkron/protoactor-dotnet), maintained at
[https://github.com/bencz/protoactor-dotnet](https://github.com/bencz/protoactor-dotnet). It focuses on running virtual
actors (grains) in scalable environments such as Kubernetes with autoscaling.

## Differences from upstream

- **.NET 10 only** — all libraries, tests, examples and benchmarks target `net10.0`.
- **PostgreSQL providers** — `Proto.Cluster.Identity.PostgreSql` (identity lookup storage) and
  `Proto.Cluster.SeedNode.PostgreSql` (seed node discovery with expiring entries).
- **Safer activations**
  - An activation request that times out is retried against the same activator instead of another member, so a slow
    member no longer leads to duplicate activations.
  - A virtual actor whose `Started` handler fails is deactivated instead of restarted in a loop; the next request
    activates it again.
- **Identity storage cleanup** — leftovers of members that are gone (e.g. after the whole cluster was restarted) are
  removed by a single member; Redis keeps a member registry instead of scanning the keyspace.
- **Redis and MongoDB fixes**
  - Redis seed entries expire unless refreshed (requires Redis/Valkey 7.4+), and removing a member no longer leaves
    keys behind.
  - MongoDB seed members can register concurrently, and the stale lock wait is configurable.
- **Removed** — Couchbase and DynamoDB persistence, Amazon ECS and Azure Container Apps cluster providers.
- **No NuGet packages** — this fork is not published to NuGet (see [Installing](#installing)).

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

- [CODEBASE_OVERVIEW.md](CODEBASE_OVERVIEW.md) – overview of the repository structure and key concepts.
- [CLUSTER_MEMBERSHIP_GOSSIP.md](CLUSTER_MEMBERSHIP_GOSSIP.md) – explains how cluster membership is detected and propagated via gossip.
- [EVENTSTREAM_EVENTS.md](EVENTSTREAM_EVENTS.md) – lists key EventStream events and their publishers/subscribers.
- [SECURITY.md](SECURITY.md) – security policy and supported versions.
- [Terminology.md](Terminology.md) – definitions of common Proto.Actor terms.

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

## Contributors

<a href="https://github.com/bencz/protoactor-dotnet/graphs/contributors">
  <img src="https://contributors-img.web.app/image?repo=bencz/protoactor-dotnet" />
</a>

Made with [contributors-img](https://contributors-img.web.app).

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
