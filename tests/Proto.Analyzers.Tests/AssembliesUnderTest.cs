using System.Reflection;
using Google.Protobuf;

namespace Proto.Analyzers.Tests;

internal static class AssembliesUnderTest
{
    public static readonly Assembly[] Assemblies =
    {
        typeof(IActor).Assembly,
        typeof(IBufferMessage).Assembly,
    };

    // The assemblies under test are built for net10.0, so the test compilation must use matching reference assemblies
    public static readonly ReferenceAssemblies ReferenceAssemblies = new(
        "net10.0",
        new PackageIdentity("Microsoft.NETCore.App.Ref", "10.0.0"),
        Path.Combine("ref", "net10.0")
    );
}