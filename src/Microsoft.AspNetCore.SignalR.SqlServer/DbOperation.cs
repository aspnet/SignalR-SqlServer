// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Microsoft.AspNetCore.SignalR.SqlServer
{
    internal class DbOperation
    {
        private List<DbParameter> _parameters = new List<DbParameter>();
        private readonly IDbProviderFactory _dbProviderFactory;

        public DbOperation(string connectionString, string commandText, ILogger logger)
            : this(connectionString, commandText, logger, SqlClientFactory.Instance.AsIDbProviderFactory())
        {

        }

        public DbOperation(string connectionString, string commandText, ILogger logger, IDbProviderFactory dbProviderFactory)
        {
            ConnectionString = connectionString;
            CommandText = commandText;
            Logger = logger;
            _dbProviderFactory = dbProviderFactory;
        }

        public DbOperation(string connectionString, string commandText, ILogger logger, params DbParameter[] parameters)
            : this(connectionString, commandText, logger)
        {
            if (parameters != null)
            {
                _parameters.AddRange(parameters);
            }
        }

        public string LoggerPrefix { get; set; }

        public IList<DbParameter> Parameters
        {
            get { return _parameters; }
        }

        protected ILogger Logger { get; private set; }

        protected string ConnectionString { get; private set; }

        protected string CommandText { get; private set; }

        public virtual object ExecuteScalar()
        {
            return Execute(cmd => cmd.ExecuteScalar());
        }

        public virtual int ExecuteNonQuery()
        {
            return Execute(cmd => cmd.ExecuteNonQuery());
        }

        public virtual Task<int> ExecuteNonQueryAsync()
        {
            return Execute(cmd => cmd.ExecuteNonQueryAsync());
        }

#if NET451
        public virtual int ExecuteReader(Action<IDataRecord, DbOperation> processRecord)
#else
        public virtual int ExecuteReader(Action<DbDataReader, DbOperation> processRecord)
#endif
        {
            return ExecuteReader(processRecord, null);
        }

#if NET451
        protected virtual int ExecuteReader(Action<IDataRecord, DbOperation> processRecord, Action<IDbCommand> commandAction)
#else
        protected virtual int ExecuteReader(Action<DbDataReader, DbOperation> processRecord, Action<DbCommand> commandAction)
#endif      
        {
            return Execute(cmd =>
            {
                if (commandAction != null)
                {
                    commandAction(cmd);
                }

                var reader = cmd.ExecuteReader();
                var count = 0;

                while (reader.Read())
                {
                    count++;
                    processRecord(reader, this);
                }

                return count;
            });
        }

#if NET451
        protected virtual IDbCommand CreateCommand(IDbConnection connection)
#else
        protected virtual DbCommand CreateCommand(DbConnection connection)
#endif
        {
            var command = connection.CreateCommand();
            command.CommandText = CommandText;

            if (Parameters != null && Parameters.Count > 0)
            {
                for (var i = 0; i < Parameters.Count; i++)
                {
                    command.Parameters.Add(Parameters[i].Clone(_dbProviderFactory));
                }
            }

            return command;
        }

#if NET451
        private T Execute<T>(Func<IDbCommand, T> commandFunc)
#else
        private T Execute<T>(Func<DbCommand, T> commandFunc)
#endif
        {
            using (var connection = _dbProviderFactory.CreateConnection())
            {
                connection.ConnectionString = ConnectionString;
                var command = CreateCommand(connection);
                connection.Open();
                LoggerCommand(command);
                return commandFunc(command);
            }
        }

#if NET451
        private void LoggerCommand(IDbCommand command)
#else
        private void LoggerCommand(DbCommand command)
#endif
        {
            if (Logger.IsEnabled(LogLevel.Debug))
            {
                Logger.LogDebug(String.Format("Created DbCommand: CommandType={0}, CommandText={1}, Parameters={2}", command.CommandType, command.CommandText,
                    command.Parameters.Cast<DbParameter>()
                        .Aggregate(string.Empty, (msg, p) => string.Format(CultureInfo.InvariantCulture, "{0} [Name={1}, Value={2}]", msg, p.ParameterName, p.Value)))
                );
            }
        }

#if NET451
        private async Task<T> Execute<T>(Func<IDbCommand, Task<T>> commandFunc)
#else
        private async Task<T> Execute<T>(Func<DbCommand, Task<T>> commandFunc)
#endif
        {
            using (var connection = _dbProviderFactory.CreateConnection())
            {
                connection.ConnectionString = ConnectionString;
                var command = CreateCommand(connection);

                connection.Open();

                try
                {
                    return await commandFunc(command);
                }
                catch (Exception ex)
                {
                    Logger.LogWarning(0, ex, "Exception thrown by Task");
                    throw;
                }
            }
        }
    }
}
