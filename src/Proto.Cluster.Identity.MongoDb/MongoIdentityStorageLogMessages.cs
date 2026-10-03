using System;
using Microsoft.Extensions.Logging;

namespace Proto.Cluster.Identity.MongoDb;

internal static partial class MongoIdentityStorageLogMessages
{
    [LoggerMessage(0, LogLevel.Debug, "Storing activation: {ActivatorId}, {SpawnLock}, {Pid}")]
    internal static partial void StoringActivation(this ILogger logger, string activatorId, SpawnLock spawnLock,
        PID pid);

    [LoggerMessage(1, LogLevel.Debug, "Removing activation: {ClusterIdentity} {Pid}")]
    internal static partial void RemovingActivation(this ILogger logger, ClusterIdentity clusterIdentity, PID pid);

    [LoggerMessage(2, LogLevel.Debug, "Got lock for {ClusterIdentity}")]
    internal static partial void GotLock(this ILogger logger, ClusterIdentity clusterIdentity);

    [LoggerMessage(3, LogLevel.Debug, "Did not get lock for {ClusterIdentity}, it is already locked or activated")]
    internal static partial void DidNotGetLock(this ILogger logger, ClusterIdentity clusterIdentity);

    [LoggerMessage(4, LogLevel.Warning, "Mongo connection failure while looking up key {Key}")]
    internal static partial void ConnectionFailureOnLookup(this ILogger logger, Exception exception, string key);
}
