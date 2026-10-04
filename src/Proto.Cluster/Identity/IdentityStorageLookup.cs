using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Proto.Cluster.Identity;

/// <summary>
///     <see cref="IIdentityLookup" /> implementation that uses external database for storing and retrieving identities.
///     See the <a href="https://proto.actor/docs/cluster/db-identity-lookup/">documentation</a> for more information.
/// </summary>
public class IdentityStorageLookup : IIdentityLookup
{
    private const string WorkerActorName = "$identity-storage-worker";
    private const string PlacementActorName = "$placement-activator";

    /// <summary>
    ///     Default time a member waits before removing leftovers of members that are not part of the cluster.
    /// </summary>
    public static readonly TimeSpan DefaultStaleMemberSweepDelay = TimeSpan.FromSeconds(30);

    private static readonly ILogger Logger = Log.CreateLogger<IdentityStorageLookup>();
    private readonly TimeSpan _staleMemberSweepDelay;
    private bool _isClient;
    private string _memberId = string.Empty;
    private PID _placementActor = null!;
    private ActorSystem _system = null!;
    private PID _worker = null!;
    internal Cluster Cluster = null!;
    internal MemberList MemberList = null!;

    public IdentityStorageLookup(IIdentityStorage storage) : this(storage, DefaultStaleMemberSweepDelay)
    {
    }

    /// <param name="storage">Storage for the activations</param>
    /// <param name="staleMemberSweepDelay">
    ///     After joining the cluster, each member looks for activations owned by members that are not part of the
    ///     cluster (e.g. left behind when the whole cluster was stopped) and removes them after this delay. The delay gives
    ///     members that are still joining time to show up in the topology.
    /// </param>
    public IdentityStorageLookup(IIdentityStorage storage, TimeSpan staleMemberSweepDelay)
    {
        Storage = storage;
        _staleMemberSweepDelay = staleMemberSweepDelay;
    }

    internal IIdentityStorage Storage { get; }

    public async Task<PID?> GetAsync(ClusterIdentity clusterIdentity, CancellationToken ct)
    {
        var msg = new GetPid(clusterIdentity, ct);

        var res = await _system.Root.RequestAsync<PidResult>(_worker, msg, ct).ConfigureAwait(false);

        if (res?.IdentityBlocked == true)
        {
            throw new IdentityIsBlockedException(clusterIdentity);
        }

        return res?.Pid;
    }

    public async Task SetupAsync(Cluster cluster, string[] kinds, bool isClient)
    {
        Cluster = cluster;
        _system = cluster.System;
        _memberId = cluster.System.Id;
        MemberList = cluster.MemberList;
        _isClient = isClient;
        await Storage.Init().ConfigureAwait(false);

        var workerProps = Props.FromProducer(() => new IdentityStorageWorker(this));
        _worker = _system.Root.SpawnNamedSystem(workerProps, WorkerActorName);

        //hook up events
        cluster.System.EventStream.Subscribe<ClusterTopology>(e =>
            {
                if (e.Left.Count == 0 ||
                    !StaleMemberSweep.IsResponsibleForCleanup(_memberId, e.Members.Select(member => member.Id)))
                {
                    return;
                }

                //delete all members that have left from the lookup
                foreach (var left in e.Left)
                    //YOLO. event stream is not async
                {
                    _ = RemoveLeftMemberAsync(left.Id);
                }
            }
        );

        if (isClient)
        {
            return;
        }

        var props = Props.FromProducer(() => new IdentityStoragePlacementActor(Cluster, this));
        _placementActor = _system.Root.SpawnNamedSystem(props, PlacementActorName);

        _ = SweepStaleMembersAsync();
    }

    public async Task ShutdownAsync()
    {
        await Cluster.System.Root.StopAsync(_worker).ConfigureAwait(false);

        if (!_isClient)
        {
            await Cluster.System.Root.StopAsync(_placementActor).ConfigureAwait(false);
        }

        await RemoveMemberAsync(_memberId).ConfigureAwait(false);
    }

    public Task RemovePidAsync(ClusterIdentity clusterIdentity, PID pid, CancellationToken ct)
    {
        if (_system.Shutdown.IsCancellationRequested)
        {
            return Task.CompletedTask;
        }

        return Storage.RemoveActivation(clusterIdentity, pid, ct);
    }

    internal Task RemoveMemberAsync(string memberId) => Storage.RemoveMember(memberId, CancellationToken.None);

    private async Task RemoveLeftMemberAsync(string memberId)
    {
        try
        {
            await RemoveMemberAsync(memberId).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            // Lookups of its identities still remove them lazily, and the next member to start sweeps them
            Logger.LeftMemberCleanupFailed(e, memberId);
        }
    }

    private async Task SweepStaleMembersAsync()
    {
        var ct = _system.Shutdown;

        try
        {
            await Cluster.JoinedCluster.WaitAsync(ct).ConfigureAwait(false);
            await MemberList.TopologyConsensus(ct).ConfigureAwait(false);

            if (!StaleMemberSweep.IsResponsibleForCleanup(_memberId, MemberList.GetMembers()))
            {
                return;
            }

            var storedMemberIds = await Storage.GetMemberIds(ct).ConfigureAwait(false);
            var candidates = StaleMemberSweep.FindStaleMembers(storedMemberIds, MemberList.ContainsMemberId);

            if (candidates.IsEmpty)
            {
                return;
            }

            Logger.FoundStaleMembers(candidates.Count, _staleMemberSweepDelay);

            // Grace period: members that are still joining get time to show up in this member's topology,
            // so their activations are never mistaken for leftovers
            await Task.Delay(_staleMemberSweepDelay, ct).ConfigureAwait(false);

            foreach (var memberId in StaleMemberSweep.FindStaleMembers(candidates, MemberList.ContainsMemberId))
            {
                ct.ThrowIfCancellationRequested();
                Logger.RemovingStaleMember(memberId);
                await RemoveMemberAsync(memberId).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // The system is shutting down, the next member to start will sweep instead
        }
        catch (Exception e)
        {
            Logger.StaleMemberSweepFailed(e);
        }
    }

    internal PID RemotePlacementActor(string address) => PID.FromAddress(address, PlacementActorName);
}