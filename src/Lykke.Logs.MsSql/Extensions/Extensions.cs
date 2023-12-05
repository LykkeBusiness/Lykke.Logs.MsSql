using System;
using System.Data;
using Dapper;

namespace Lykke.Logs.MsSql.Extensions
{
    public static class Extensions
    {
        /// <summary>
        /// Validates that required SQL table exists.
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="createQuery"></param>
        /// <param name="tableName"></param>
        /// <param name="schemaName"> Optional. Must be set if non-dbo schema is used,
        /// in that case <paramref name="tableName"/> must contain no schema.</param>
        /// <exception cref="ArgumentException">
        /// When <paramref name="createQuery"/> is empty or doesn't contain {0} placeholder for table name.
        /// When <paramref name="tableName"/> is empty.
        /// </exception>
        /// <remarks>
        /// It is expected that <paramref name="createQuery"/> contains {0} placeholder for table name.
        /// </remarks>
        public static void CreateTableIfDoesntExists(this IDbConnection connection, string createQuery,
            string tableName, string schemaName = null)
        {
            if (string.IsNullOrEmpty(createQuery))
                throw new ArgumentException($"{nameof(createQuery)} must be not empty");
            
            if (!createQuery.Contains("{0}"))
                throw new ArgumentException($"{nameof(createQuery)} must contain {0} placeholder for table name");
            
            if (string.IsNullOrEmpty(tableName))
                throw new ArgumentException($"{nameof(tableName)} must be not empty");
            
            var fullTableName = tableName;
            connection.Open();
            try
            {
                // Check if schema exists if required
                if (!string.IsNullOrEmpty(schemaName))
                {
                    fullTableName = $"{schemaName}.{fullTableName}";

                    if (1 != connection.ExecuteScalar<int>(
                            "SELECT 1 FROM information_schema.schemata WHERE schema_name = @schemaName",
                            new { schemaName }))
                    {
                        // Create schema
                        var query = $"CREATE SCHEMA {schemaName} AUTHORIZATION dbo";
                        connection.Query(query);
                    }
                }

                // Check if table exists
                connection.ExecuteScalar($"select top 1 * from {fullTableName}");
            }
            //avolkov this method could be called with System.Data.SqlClient or Microsoft.Data.SqlClient
            //so we should catch both exception to be able to create table if not exist
            catch (Exception ex) when (ex is System.Data.SqlClient.SqlException ||
                                       ex is Microsoft.Data.SqlClient.SqlException)
            {
                // Create table
                var query = string.Format(createQuery, fullTableName);
                connection.Query(query);
            }
            finally
            {
                connection.Close();
            }
        }
    }
}
