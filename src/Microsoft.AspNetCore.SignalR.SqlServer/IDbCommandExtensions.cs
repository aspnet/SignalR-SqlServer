// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Threading.Tasks;
using JetBrains.Annotations;


namespace Microsoft.AspNetCore.SignalR.SqlServer
{
    internal static class IDbCommandExtensions
    {
        private readonly static TimeSpan _dependencyTimeout = TimeSpan.FromSeconds(60);

#if NET451
        public static void AddSqlDependency([NotNull]this IDbCommand command, Action<SqlNotificationEventArgs> callback)
        {
            var sqlCommand = command as SqlCommand;
            if (sqlCommand == null)
            {
                throw new NotSupportedException();
            }

            var dependency = new SqlDependency(sqlCommand, null, (int)_dependencyTimeout.TotalSeconds);
            dependency.OnChange += (o, e) => callback(e);
        }
#endif

#if NET451
        public static Task<int> ExecuteNonQueryAsync(this IDbCommand command)
#else
        public static Task<int> ExecuteNonQueryAsync(this DbCommand command)
#endif
        {
            var sqlCommand = command as SqlCommand;

            if (sqlCommand != null)
            {
#if NET451
                return Task.Factory.FromAsync(
                    (cb, state) => sqlCommand.BeginExecuteNonQuery(cb, state),
                    iar => sqlCommand.EndExecuteNonQuery(iar),
                    null);
#else
                return sqlCommand.ExecuteNonQueryAsync();
#endif
            }
            else
            {
                return Task.FromResult(command.ExecuteNonQuery());
            }
        }
    }
}
