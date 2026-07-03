using System.Collections.Immutable;

namespace JorgeCostaMacia.Bus.UrnFactory.Tests;

public class UrnFactoryTests
{
    private interface IMarker;

    private class GrandParent;

    private class Parent : GrandParent;

    private sealed class Child : Parent, IMarker, IDisposable
    {
        public void Dispose() { }
    }

    private static string Urn(Type type) => $"urn:message:{type.FullName}";

    [Fact]
    public void Create_IncludesTheTypeItself()
        => Assert.Contains(Urn(typeof(Child)), Domain.UrnFactory.Create<Child>());

    [Fact]
    public void Create_IncludesNonSystemInterfaces_AndExcludesSystemOnes()
    {
        ImmutableList<string> urns = Domain.UrnFactory.Create<Child>();

        Assert.Contains(Urn(typeof(IMarker)), urns);
        Assert.DoesNotContain($"urn:message:{typeof(IDisposable).FullName}", urns);
    }

    [Fact]
    public void Create_DefaultDeep_IncludesTwoBaseTypes()
    {
        ImmutableList<string> urns = Domain.UrnFactory.Create<Child>();

        Assert.Contains(Urn(typeof(Parent)), urns);
        Assert.Contains(Urn(typeof(GrandParent)), urns);
    }

    [Fact]
    public void Create_DeepOne_StopsAtTheFirstBaseType()
    {
        ImmutableList<string> urns = Domain.UrnFactory.Create<Child>(deep: 1);

        Assert.Contains(Urn(typeof(Parent)), urns);
        Assert.DoesNotContain(Urn(typeof(GrandParent)), urns);
    }

    [Fact]
    public void Create_NullDeep_IncludesNoBaseTypes()
    {
        ImmutableList<string> urns = Domain.UrnFactory.Create<Child>(deep: null);

        Assert.Contains(Urn(typeof(Child)), urns);
        Assert.DoesNotContain(Urn(typeof(Parent)), urns);
    }

    [Fact]
    public void Create_NeverIncludesObject()
        => Assert.DoesNotContain("urn:message:System.Object", Domain.UrnFactory.Create<Child>(deep: 10));

    [Fact]
    public void Create_OrdersByUrnLength()
    {
        ImmutableList<string> urns = Domain.UrnFactory.Create<Child>();

        Assert.Equal(urns.OrderBy(urn => urn.Length), urns);
    }

    [Fact]
    public void Create_NullType_ReturnsEmpty()
        => Assert.Empty(Domain.UrnFactory.Create(null!));

    [Fact]
    public void Create_ByType_MatchesTheGenericOverload()
        => Assert.Equal(Domain.UrnFactory.Create<Child>(), Domain.UrnFactory.Create(typeof(Child)));
}
