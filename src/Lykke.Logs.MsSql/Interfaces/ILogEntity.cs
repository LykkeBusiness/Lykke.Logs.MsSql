using System;

namespace Lykke.Logs.MsSql.Interfaces
{
    public interface ILogEntity
    {
        DateTime DateTime { get; }
        string Level { get; }
        string Env { get; }
        string AppName { get; }
        string Version { get; }
        string Component { get; }
        string Process { get; }
        string Context { get; }
        string Type { get; }
        string Stack { get; }
        string Msg { get; }
    }
}