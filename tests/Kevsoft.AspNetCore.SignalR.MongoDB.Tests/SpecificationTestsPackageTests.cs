using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Specification.Tests;

namespace Kevsoft.AspNetCore.SignalR.MongoDB.Tests;

public class SpecificationTestsPackageTests
{
    [Fact]
    public void SpecificationTestsPackageIsAvailable()
    {
        var type = typeof(HubLifetimeManagerTestsBase<Hub>);

        Assert.Equal("Microsoft.AspNetCore.SignalR.Specification.Tests", type.Assembly.GetName().Name);
    }
}
