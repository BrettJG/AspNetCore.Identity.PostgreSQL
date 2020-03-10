using AspNetCore.Identity.PostgreSQL.Context;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace AspNetCore.Identity.PostgreSQL.Tables
{
    public class UserTokenTable<T> where T : IEquatable<T>
    {
        private const string _tableName = "AspNetUserTokens";

        private readonly PostgreSQLDatabase _database;

        public UserTokenTable(PostgreSQLDatabase database)
        {
            _database = database;
        }


        public async Task<int> AddUserTokenAsync(IdentityUserToken<T> token)
        {
            var sqlCommand = $"INSERT INTO \"{_tableName}\" (\"UserId\", \"LoginProvider\", \"Name\", \"Value\") VALUES (@UserId, @LoginProvider, @Name, @Value)";
            var sqlParams = new Dictionary<string, object>();
            sqlParams.Add("UserId", token.UserId);
            sqlParams.Add("LoginProvider", token.LoginProvider);
            sqlParams.Add("Name", token.Name);
            sqlParams.Add("Value", token.Value);

            return await _database.ExecuteSQLAsync(sqlCommand, sqlParams);
        }

        public async Task<int> RemoveUserTokenAsync(IdentityUserToken<T> token)
        {
            var sqlCommand = $"DELETE FROM \"{_tableName}\" WHERE \"UserId\" = @UserId AND \"LoginProvider\" = @LoginProvider AND \"Name\" = @Name ";
            var sqlParams = new Dictionary<string, object>();
            sqlParams.Add("UserId", token.UserId);
            sqlParams.Add("LoginProvider", token.LoginProvider);
            sqlParams.Add("Name", token.Name);

            return await _database.ExecuteSQLAsync(sqlCommand, sqlParams);
        }

        public async Task<IdentityUserToken<T>> FindUserTokenAsync(IdentityUser user, string loginProvider, string name)
        {
            var sqlCommand = $"SELECT UserId, LoginProvider, Name, Value FROM \"{_tableName}\" WHERE \"UserId\" = @UserId AND \"LoginProvider\" = @LoginProvider AND \"Name\" = @Name";
            var sqlParams = new Dictionary<string, object>();
            sqlParams.Add("UserId", user);
            sqlParams.Add("LoginProvider", loginProvider);
            sqlParams.Add("Name", name);

            using (var reader = await _database.ExecuteQueryAsync(sqlCommand, sqlParams))
            {
                while (reader.Read())
                {
                    var token = new IdentityUserToken<T>();
                    token.LoginProvider = reader[nameof(token.LoginProvider)].ToString();
                    token.Name = reader[nameof(token.Name)].ToString();
                    token.UserId = (T)Convert.ChangeType(reader[nameof(token.UserId)].ToString(), typeof(T));
                    token.Value = reader[nameof(token.Value)].ToString();

                    return token;
                }
            }

            return null;
        }
    }
}
