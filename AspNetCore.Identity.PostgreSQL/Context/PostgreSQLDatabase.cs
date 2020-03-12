using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json.Linq;
using Npgsql;
using NpgsqlTypes;

namespace AspNetCore.Identity.PostgreSQL.Context
{
    public class PostgreSQLDatabase
    {
        //private static NpgsqlConnection _connection;
        private readonly IConfiguration _configurationRoot;
        //private static object _consulta = new object();

        public PostgreSQLDatabase(IConfiguration configurationRoot)
        {
            _configurationRoot = configurationRoot;
            if (string.IsNullOrEmpty(IdentityDbConfig.StringConnectionName))
                IdentityDbConfig.StringConnectionName = "PostgreSQLBaseConnection";
            
        }


        public int ExecuteSQL(string commandText, Dictionary<string, object> parameters)
        {          
            var result = 0;

            if (string.IsNullOrEmpty(commandText))
            {
                throw new ArgumentException("Command text cannot be null or empty.");
            }


            using (var conn = CreateConnection())
            {
                using (var command = CreateCommand(conn, commandText, parameters))
                {
                    result = command.ExecuteNonQuery();
                }
            }
                
            return result;   
        }

        public async Task<int> ExecuteSQLAsync(string commandText, Dictionary<string, object> parameters)
        {
            if (string.IsNullOrEmpty(commandText))
                throw new ArgumentException("Command text cannot be null or empty.");
            
            using (var conn = CreateConnection())
            {
                using (var command = CreateCommand(conn, commandText, parameters))
                {
                    return await command.ExecuteNonQueryAsync();
                }
            }
        }

        public object ExecuteQueryGetSingleObject(string commandText, Dictionary<string, object> parameters)
        {
            object result = null;

            if (string.IsNullOrEmpty(commandText))
            {
                throw new ArgumentException("Command text cannot be null or empty.");
            }


            using (var conn = CreateConnection())
            {
                using (var command = CreateCommand(conn, commandText, parameters))
                {
                    result = command.ExecuteScalar();
                }
            }            

            return result;            
        }

        public Dictionary<string, string> ExecuteQueryGetSingleRow(string commandText,
            Dictionary<string, object> parameters)
        {
    
            Dictionary<string, string> row = null;
            if (string.IsNullOrEmpty(commandText))
            {
                throw new ArgumentException("Command text cannot be null or empty.");
            }


            using (var conn = CreateConnection())
            {
                using (var command = CreateCommand(conn, commandText, parameters))
                {
                    using (NpgsqlDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            row = new Dictionary<string, string>();
                            for (var i = 0; i < reader.FieldCount; i++)
                            {
                                var columnName = reader.GetName(i);
                                var columnValue = reader.IsDBNull(i) ? null : reader.GetValue(i).ToString();
                                row.Add(columnName, columnValue);
                            }
                            break;
                        }
                    }
                }
            }
           

            return row;
            
        }

        public async Task<Dictionary<string, object>> ExecuteQueryGetSingleRowAsync(string commandText,
            Dictionary<string, object> parameters)
        {

            Dictionary<string, object> row = null;
            if (string.IsNullOrEmpty(commandText))
            {
                throw new ArgumentException("Command text cannot be null or empty.");
            }


            using (var conn = CreateConnection())
            {
                using (var command = CreateCommand(conn, commandText, parameters))
                {
                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (reader.Read())
                        {
                            row = new Dictionary<string, object>();
                            for (var i = 0; i < reader.FieldCount; i++)
                            {
                                var columnName = reader.GetName(i);
                                var columnValue = reader.IsDBNull(i) ? null : reader.GetValue(i);
                                row.Add(columnName, columnValue);
                            }
                            break;
                        }
                    }
                }
            }

            return row;
        }


        public List<Dictionary<string, string>> ExecuteQuery(string commandText, Dictionary<string, object> parameters)
        {
           
            List<Dictionary<string, string>> rows = null;
            if (string.IsNullOrEmpty(commandText))
            {
                throw new ArgumentException("Command text cannot be null or empty.");
            }

              
            using (var conn = CreateConnection())
            {
                using (var command = CreateCommand(conn, commandText, parameters))
                {
                    using (NpgsqlDataReader reader = command.ExecuteReader())
                    {
                        rows = new List<Dictionary<string, string>>();
                        while (reader.Read())
                        {
                            var row = new Dictionary<string, string>();
                            for (var i = 0; i < reader.FieldCount; i++)
                            {
                                var columnName = reader.GetName(i);
                                var columnValue = reader.IsDBNull(i) ? null : reader.GetValue(i).ToString();
                                row.Add(columnName, columnValue);
                            }
                            rows.Add(row);
                        }
                    }
                }
            }
               

            return rows;
        }

        private NpgsqlConnection CreateConnection()
        {
            var conn = new NpgsqlConnection(_configurationRoot.GetConnectionString(IdentityDbConfig.StringConnectionName));
            conn.Open();
            return conn;
        }

        private NpgsqlCommand CreateCommand(NpgsqlConnection connection, string commandText, Dictionary<string, object> parameters)
        {

            NpgsqlCommand command = connection.CreateCommand();
            command.CommandText = commandText;
            AddParameters(command, parameters);

            return command;

        }

        /// <summary>
        ///     Adds the parameters to a PostgreSQL command.
        /// </summary>
        /// <param name="commandText">The PostgreSQL query to execute.</param>
        /// <param name="parameters">Parameters to pass to the PostgreSQL query.</param>
        private static void AddParameters(NpgsqlCommand command, Dictionary<string, object> parameters)
        {
            if (parameters == null)
            {
                return;
            }

            foreach (var param in parameters)
            {
                var parameter = command.CreateParameter();
                parameter.ParameterName = param.Key;
                parameter.Value = param.Value ?? DBNull.Value;

                var s = param.Value as string;
                if ((s != null) && s.StartsWith("JSON"))
                {
                    parameter.Value = JObject.Parse(s.Replace("JSON", ""));
                    parameter.NpgsqlDbType = NpgsqlDbType.Json;
                }

                command.Parameters.Add(parameter);
            }
        }


    

      
        public void Dispose()
        {

        }
    }
}