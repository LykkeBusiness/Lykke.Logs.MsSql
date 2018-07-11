using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Linq;
using Dapper;
using Lykke.Logs.MsSql.Extensions;

namespace Lykke.Logs.MsSql
{
    public class LogMsSql : ILogMsSql
    {
        private readonly string _tableName;
        private const string CreateTableScript = "CREATE TABLE [{0}](" +
                                                 "[Id] [bigint] NOT NULL IDENTITY(1,1) PRIMARY KEY," +
                                                 "[DateTime] [DateTime] NOT NULL," +
                                                 "[Level] [nvarchar] (64) NOT NULL, " +
                                                 "[Env] [nvarchar] (64) NULL, " +
                                                 "[AppName] [nvarchar] (256) NULL, " +
                                                 "[Version] [nvarchar] (256) NULL, " +
                                                 "[Component] [nvarchar] (1024) NULL, " +
                                                 "[Process] [nvarchar] (1024) NOT NULL, " +
                                                 "[Context] [nvarchar] (MAX) NOT NULL, " +
                                                 "[Type] [nvarchar] (1024) NOT NULL, " +
                                                 "[Stack] [nvarchar] (MAX) NULL, " +
                                                 "[Msg] [nvarchar] (MAX) NULL " +
                                                 ");";

        private static Type DataType => typeof(ILogObject);
        private static readonly string GetColumns = "[" + string.Join("],[", DataType.GetProperties().Select(x => x.Name)) + "]";
        private static readonly string GetFields = string.Join(",", DataType.GetProperties().Select(x => "@" + x.Name));

        private readonly string _connectionString;

        public LogMsSql(string logTableName, string connectionString)
        {
            _tableName = logTableName;
            _connectionString = connectionString;

            using (var conn = new SqlConnection(_connectionString))
            {
                conn.CreateTableIfDoesntExists(CreateTableScript, _tableName);
            }
        }

        public async Task Insert(ILogObject log)
        {
            using (var conn = new SqlConnection(_connectionString))
            {
                await conn.ExecuteAsync(
                    $"insert into {_tableName} ({GetColumns}) values ({GetFields})", log);
            }
        }

    }
}
