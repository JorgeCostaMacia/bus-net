namespace JorgeCostaMacia.Bus.Domain.Contexts;

/// <summary>
/// Envelope facet surfacing the host that produced the message — machine, process, assembly, runtime
/// and bus version — read from the transport header without deserializing the body. Stamped by the
/// outbound gate on every produce, so a normal message carries its sender's host and a parked
/// failure carries the host of the consumer that failed.
/// </summary>
public interface IHostContext : IContext
{
    /// <summary>The machine (host) name — in a container, its hostname.</summary>
    string HostMachineName { get; }

    /// <summary>The entry assembly's simple name — the application.</summary>
    string HostAssembly { get; }

    /// <summary>The entry assembly's version.</summary>
    string HostAssemblyVersion { get; }

    /// <summary>The .NET runtime version the host runs on.</summary>
    string HostFrameworkVersion { get; }

    /// <summary>The bus library version the host runs.</summary>
    string HostBusVersion { get; }

    /// <summary>The operating system version.</summary>
    string HostOperatingSystemVersion { get; }
}
