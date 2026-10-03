using FluentAssertions;
using Proto.Cluster.Identity.Redis;
using Xunit;

namespace Proto.Cluster.RedisIdentity.Tests;

public class RedisPatternsTests
{
    [Theory]
    [InlineData("my-cluster:mb:", "my-cluster:mb:")]
    [InlineData("cluster*:mb:", "cluster\\*:mb:")]
    [InlineData("a?b[c]d\\e", "a\\?b\\[c\\]d\\\\e")]
    public void EscapesGlobCharacters(string literal, string expected) =>
        RedisPatterns.Escape(literal).Should().Be(expected);
}
