﻿using System;
using System.Threading.Tasks;
using Common;
using Common.Log;
using Lykke.Common.Log;
using Microsoft.Extensions.PlatformAbstractions;
using Microsoft.Extensions.Logging;
using LogLevel = Lykke.Logs.MsSql.Domain.LogLevel;
using Lykke.Logs.MsSql.Interfaces;

namespace Lykke.Logs.MsSql
{
    //TODO: to refactor and use new logs mechanism
    public class LogToSql : ILog
    {
        private readonly ILogRepository _logRepository;

        public LogToSql(
            ILogRepository logRepository)
        {
            _logRepository = logRepository;
        }

        private Task WriteLog(LogLevel level, string component, string process, string context, string info, 
            Exception ex = null, DateTime? dateTime = null)
        {
#pragma warning disable 4014
            Task.Run(async () =>
#pragma warning restore 4014
            {
                var log = new LogEntity
                {
                    DateTime = dateTime ?? DateTime.UtcNow,
                    Level = level.ToString(),
                    Env = Environment.GetEnvironmentVariable("ENV_INFO"),
                    AppName = PlatformServices.Default.Application.ApplicationName,
                    Version = PlatformServices.Default.Application.ApplicationVersion,
                    Component = component,
                    Process = process,
                    Context = context,
                    Type = "Message",
                    Stack = Truncate(ex?.StackTrace),
                    Msg = string.Join(" *** ", info, Truncate(ex?.Message)),
                };

                await _logRepository.Insert(log);
            });

            return Task.CompletedTask;
        }

        public void Log<TState>(Microsoft.Extensions.Logging.LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter) where TState : LogEntryParameters
        {
            WriteLog(Map(logLevel), "", "", state.ToJson(), formatter(state, exception), exception);
        }

        private LogLevel Map(Microsoft.Extensions.Logging.LogLevel logLevel)
        {
            switch (logLevel)
            {
                case Microsoft.Extensions.Logging.LogLevel.Debug:
                case Microsoft.Extensions.Logging.LogLevel.Trace:
                case Microsoft.Extensions.Logging.LogLevel.Information:
                    return LogLevel.Info;
                
                case Microsoft.Extensions.Logging.LogLevel.Warning:
                    return LogLevel.Warning;
                
                case Microsoft.Extensions.Logging.LogLevel.Error:
                    return LogLevel.Error;
                
                case Microsoft.Extensions.Logging.LogLevel.Critical:
                    return LogLevel.FatalError;
                
                default:
                    return LogLevel.None;
            }
        }

        public bool IsEnabled(Microsoft.Extensions.Logging.LogLevel logLevel)
        {
            return true;
        }

        public IDisposable BeginScope(string scopeMessage)
        {
            return new DummyDisposableObject();
        }

        private class DummyDisposableObject : IDisposable
        {
            public void Dispose()
            {
                
            }
        }

        public async Task WriteInfoAsync(string component, string process, string context, string info, DateTime? dateTime = null)
        {
            await WriteLog(LogLevel.Info, component, process, context, info, null, dateTime);
        }

        public async Task WriteMonitorAsync(string component, string process, string context, string info, DateTime? dateTime = null)
        {
            await WriteLog(LogLevel.Monitoring, component, process, context, info, null, dateTime);
        }

        public async Task WriteWarningAsync(string component, string process, string context, string info, DateTime? dateTime = null)
        {
            await WriteLog(LogLevel.Warning, component, process, context, info, null, dateTime);
        }

        public async Task WriteWarningAsync(string component, string process, string context, string info, Exception ex,
            DateTime? dateTime = null)
        {
            await WriteLog(LogLevel.Warning, component, process, context, info, ex, dateTime);
        }

        public async Task WriteErrorAsync(string component, string process, string context, Exception exception, DateTime? dateTime = null)
        {
            await WriteLog(LogLevel.Error, component, process, context, null, exception, dateTime);
        }

        public async Task WriteFatalErrorAsync(string component, string process, string context, Exception exception,
            DateTime? dateTime = null)
        {
            await WriteLog(LogLevel.FatalError, component, process, context, null, exception, dateTime);
        }

        public async Task WriteInfoAsync(string process, string context, string info, DateTime? dateTime = null)
        {
            await WriteLog(LogLevel.Info, null, process, context, info, null, dateTime);
        }

        public async Task WriteMonitorAsync(string process, string context, string info, DateTime? dateTime = null)
        {
            await WriteLog(LogLevel.Monitoring, null, process, context, info, null, dateTime);
        }

        public async Task WriteWarningAsync(string process, string context, string info, DateTime? dateTime = null)
        {
            await WriteLog(LogLevel.Warning, null, process, context, info, null, dateTime);
        }

        public async Task WriteWarningAsync(string process, string context, string info, Exception ex, DateTime? dateTime = null)
        {
            await WriteLog(LogLevel.Warning, null, process, context, info, ex, dateTime);
        }

        public async Task WriteErrorAsync(string process, string context, Exception exception, DateTime? dateTime = null)
        {
            await WriteLog(LogLevel.Error, null, process, context, null, exception, dateTime);
        }

        public async Task WriteFatalErrorAsync(string process, string context, Exception exception, DateTime? dateTime = null)
        {
            await WriteLog(LogLevel.FatalError, null, process, context, null, exception, dateTime);
        }
        
        private static string Truncate(string str)
        {
            if (str == null)
            {
                return null;
            }

            // See: https://blogs.msdn.microsoft.com/avkashchauhan/2011/11/30/how-the-size-of-an-entity-is-caclulated-in-windows-azure-table-storage/
            // String – # of Characters * 2 bytes + 4 bytes for length of string
            // Max column size is 64 Kb, so max string len is (65536 - 4) / 2 = 32766
            // 3 - is for "..."
            const int maxLength = 32766 - 3;

            if (str.Length > maxLength)
            {
                return string.Concat(str.Substring(0, maxLength), "...");
            }

            return str;
        }
    }
}
