using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.Extensions.Logging;
using Proto.Cluster.Seed;
using StackExchange.Redis;

namespace Proto.Cluster.SeedNode.Redis;

/// <summary>
///     Stores the seed node members in a Redis hash. Every entry expires after <c>memberTtl</c> unless the member that
///     registered it keeps refreshing it, so members that die without deregistering (crash, SIGKILL, lost node) disappear
///     on their own. Only the entries this instance registers get an expiration: entries written by other writers sharing
///     the hash (older versions, other applications) are left untouched and must be removed by their owners.
///     Requires Redis or Valkey 7.4+ (hash field expiration).
/// </summary>
[PublicAPI]
public class RedisSeedNodeDiscovery : ISeedNodeDiscovery, IDisposable
{
    public static readonly TimeSpan DefaultMemberTtl = TimeSpan.FromSeconds(30);

    private static readonly ILogger Logger = Log.CreateLogger<RedisSeedNodeDiscovery>();

    private readonly IDatabase _db;
    private readonly ConcurrentDictionary<string, Heartbeat> _heartbeats = new();
    private readonly RedisKey _key;
    private readonly TimeSpan _memberTtl;

    /// <param name="multiplexer">Redis connection</param>
    /// <param name="storageKey">Key of the hash that holds the members</param>
    /// <param name="memberTtl">
    ///     How long an entry survives without being refreshed. Registered members refresh their entry every third of
    ///     this period. Defaults to <see cref="DefaultMemberTtl" />.
    /// </param>
    public RedisSeedNodeDiscovery(
        IConnectionMultiplexer multiplexer,
        string storageKey = "RedisSeedNode",
        TimeSpan? memberTtl = null
    )
    {
        _key = storageKey;
        _db = multiplexer.GetDatabase();
        _memberTtl = memberTtl ?? DefaultMemberTtl;
    }

    public async Task Register(string memberId, string host, int port)
    {
        RedisValue address = SeedNodeAddress.Format(host, port);
        await WriteEntryAsync(memberId, address).ConfigureAwait(false);

        var heartbeat = new Heartbeat();
        heartbeat.Task = RefreshAsync(memberId, address, heartbeat.Cancellation.Token);

        if (_heartbeats.TryRemove(memberId, out var previous))
        {
            await previous.StopAsync().ConfigureAwait(false);
        }

        _heartbeats[memberId] = heartbeat;
    }

    public async Task Remove(string memberId)
    {
        // Stop refreshing first, so an in-flight refresh cannot write the entry back after it was deleted
        if (_heartbeats.TryRemove(memberId, out var heartbeat))
        {
            await heartbeat.StopAsync().ConfigureAwait(false);
        }

        await _db.HashDeleteAsync(_key, memberId).ConfigureAwait(false);
    }

    public async Task<(string memberId, string host, int port)[]> GetAll()
    {
        var entries = await _db.HashGetAllAsync(_key).ConfigureAwait(false);
        var members = new List<(string memberId, string host, int port)>(entries.Length);

        foreach (var entry in entries)
        {
            string memberId = entry.Name!;
            string? address = entry.Value;

            if (SeedNodeAddress.TryParse(address, out var host, out var port))
            {
                members.Add((memberId, host, port));
            }
            else
            {
                Logger.InvalidAddress(memberId, address);
            }
        }

        return members.ToArray();
    }

    /// <summary>
    ///     Stops refreshing the registered members. Their entries expire after the configured TTL.
    /// </summary>
    public void Dispose()
    {
        foreach (var memberId in _heartbeats.Keys)
        {
            if (_heartbeats.TryRemove(memberId, out var heartbeat))
            {
                heartbeat.Cancellation.Cancel();
                heartbeat.Cancellation.Dispose();
            }
        }

        GC.SuppressFinalize(this);
    }

    private Task WriteEntryAsync(string memberId, RedisValue address)
    {
        var transaction = _db.CreateTransaction();
        _ = transaction.HashSetAsync(_key, memberId, address);
        _ = transaction.HashFieldExpireAsync(_key, new RedisValue[] { memberId }, _memberTtl);

        return transaction.ExecuteAsync();
    }

    private async Task RefreshAsync(string memberId, RedisValue address, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                // Refresh well before the entry expires, so a single failed write does not drop the member
                await Task.Delay(_memberTtl / 3, ct).ConfigureAwait(false);
                await WriteEntryAsync(memberId, address).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return;
            }
            catch (Exception e)
            {
                Logger.HeartbeatFailed(e, memberId);
            }
        }
    }

    private sealed class Heartbeat
    {
        public CancellationTokenSource Cancellation { get; } = new();

        public Task Task { get; set; } = Task.CompletedTask;

        public async Task StopAsync()
        {
            Cancellation.Cancel();
            await Task.ConfigureAwait(false);
            Cancellation.Dispose();
        }
    }
}
