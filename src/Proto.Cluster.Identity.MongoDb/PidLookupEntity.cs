using System;
using JetBrains.Annotations;
using MongoDB.Bson.Serialization.Attributes;

// ReSharper disable PropertyCanBeMadeInitOnly.Global
// ReSharper disable UnusedAutoPropertyAccessor.Global

namespace Proto.Cluster.Identity.MongoDb;

// Lets future versions add fields without breaking readers of this version
[BsonIgnoreExtraElements]
[UsedImplicitly]
public class PidLookupEntity
{
    [BsonId] public string Key { get; set; } = default!;

    public string Identity { get; set; } = default!;
    public string? UniqueIdentity { get; set; } = default!;
    public string Kind { get; set; } = default!;
    public string? Address { get; set; }
    public string? MemberId { get; set; }
    public string? LockedBy { get; set; }

    /// <summary>
    ///     When the spawn lock was taken, set by the database clock. A lock older than the stale lock wait is treated as
    ///     abandoned.
    /// </summary>
    public DateTime? LockedAt { get; set; }
    public int Revision { get; set; }
}