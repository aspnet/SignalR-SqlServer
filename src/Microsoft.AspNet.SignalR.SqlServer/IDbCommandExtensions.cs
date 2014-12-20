// Copyright (c) Microsoft Open Technologies, Inc. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Data.Common;
using System.Data.SqlClient;
using System.Threading.Tasks;

namespace Microsoft.AspNet.SignalR.SqlServer
{
    internal static class IDbCommandExtensions
    {
        private readonly static TimeSpan _dependencyTimeout = TimeSpan.FromSeconds(60);

#if ASPNET50
        public static void AddSqlDependency(this DbCommand command, Action<SqlNotificationEventArgs> callback)
        {
            var sqlCommand = command as SqlCommand;
            if (sqlCommand == null)
            {
                throw new NotSupportedException();
            }

            var dependency = new SqlDependency(sqlCommand, null, (int)_dependencyTimeout.TotalSeconds);
            dependency.OnChange += (o, e) => callback(e);
        }
#else
        public static void AddSqlNotification(this DbCommand command, Action callback)
        {
            var sqlCommand = command as SqlCommand;
            if (sqlCommand == null)
            {
                throw new NotSupportedException();
            }

             callback();
        }
#endif
        public static Task<int> ExecuteNonQueryAsync(this DbCommand command)
        {
            var sqlCommand = command as SqlCommand;

            if (sqlCommand != null)
            {
                return Task.Factory.FromAsync(
                    (cb, state) => sqlCommand.BeginExecuteNonQuery(cb, state),
                    iar => sqlCommand.EndExecuteNonQuery(iar),
                    null);
            }
            else
            {
                return TaskAsyncHelper.FromResult(command.ExecuteNonQuery());
            }
        }
    }
}
