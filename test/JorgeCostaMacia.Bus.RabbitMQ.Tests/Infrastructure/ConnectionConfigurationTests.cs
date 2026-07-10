using JorgeCostaMacia.Bus.RabbitMQ.Infrastructure;
using RabbitMQ.Client;

namespace JorgeCostaMacia.Bus.RabbitMQ.Tests.Infrastructure;

public class ConnectionConfigurationTests
{
    [Fact]
    public void ConnectionFactory_UnsetValues_FallBackToTheDefaults()
    {
        ConnectionFactory factory = new ConnectionConfiguration { HostName = "bus", UserName = "user", Password = "pass" }.ConnectionFactory;

        Assert.Equal("bus", factory.HostName);
        Assert.Equal("user", factory.UserName);
        Assert.Equal("pass", factory.Password);
        Assert.Equal(ConnectionConfigurationDefaults.PORT, factory.Port);
        Assert.Equal(ConnectionConfigurationDefaults.VIRTUAL_HOST, factory.VirtualHost);
        Assert.Equal(Environment.MachineName, factory.ClientProvidedName);
        Assert.Equal(ConnectionConfigurationDefaults.AUTOMATIC_RECOVERY_ENABLED, factory.AutomaticRecoveryEnabled);
    }

    [Fact]
    public void ConnectionFactory_SuppliedValues_Win()
    {
        ConnectionFactory factory = new ConnectionConfiguration
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
