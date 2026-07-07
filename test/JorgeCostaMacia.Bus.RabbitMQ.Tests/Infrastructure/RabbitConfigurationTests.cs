using JorgeCostaMacia.Bus.RabbitMQ.Infrastructure;
using RabbitMQ.Client;

namespace JorgeCostaMacia.Bus.RabbitMQ.Tests;

public class RabbitConfigurationTests
{
    [Fact]
    public void ConnectionFactory_UnsetValues_FallBackToTheDefaults()
    {
        ConnectionFactory factory = new RabbitConfiguration { HostName = "bus", UserName = "user", Password = "pass" }.ConnectionFactory;

        Assert.Equal("bus", factory.HostName);
        Assert.Equal("user", factory.UserName);
        Assert.Equal("pass", factory.Password);
        Assert.Equal(RabbitConfigurationDefaults.PORT, factory.Port);
        Assert.Equal(RabbitConfigurationDefaults.VIRTUAL_HOST, factory.VirtualHost);
        Assert.Equal(Environment.MachineName, factory.ClientProvidedName);
        Assert.Equal(RabbitConfigurationDefaults.AUTOMATIC_RECOVERY_ENABLED, factory.AutomaticRecoveryEnabled);
    }

    [Fact]
    public void ConnectionFactory_SuppliedValues_Win()
    {
        ConnectionFactory factory = new RabbitConfiguration
        {
            HostName = "bus",
            UserName = "user",
            Password = "pass",
            Port = 5673,
            VirtualHost = "vh",
            ClientProvidedName = "custom",
            AutomaticRecoveryEnabled = false
        }.ConnectionFactory;

        Assert.Equal(5673, factory.Port);
        Assert.Equal("vh", factory.VirtualHost);
        Assert.Equal("custom", factory.ClientProvidedName);
        Assert.False(factory.AutomaticRecoveryEnabled);
    }
}
