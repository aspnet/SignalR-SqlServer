// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.AspNetCore.SignalR.Messaging;
using Microsoft.Extensions.Logging;

namespace Microsoft.AspNetCore.SignalR.SqlServer
{
    internal class SqlSender
    {
        private readonly string _connectionString;
        private readonly string _insertDml;
        private readonly ILogger _logger;
        private readonly IDbProviderFactory _dbProviderFactory;

        public SqlSender(string connectionString, string tableName, ILogger logger, IDbProviderFactory dbProviderFactory)
        {
            _connectionString = connectionString;
            _insertDml = BuildInsertString(tableName);
            _logger = logger;
            _dbProviderFactory = dbProviderFactory;
        }

        private string BuildInsertString(string tableName)
        {
            var insertDml = GetType().GetTypeInfo().Assembly.StringResource("send.sql");

            return insertDml.Replace("[SignalR]", String.Format(CultureInfo.InvariantCulture, "[{0}]", SqlMessageBus.SchemaName))
                            .Replace("[Messages_0", String.Format(CultureInfo.InvariantCulture, "[{0}", tableName));
        }

        public Task Send(IList<Message> messages)
        {
            if (messages == null || messages.Count == 0)
            {
                return Task.FromResult<object>(null);
            }

            var parameter = _dbProviderFactory.CreateParameter();
            parameter.ParameterName = "Payload";
            parameter.DbType = DbType.Binary;
            parameter.Value = SqlPayload.ToBytes(messages);

            var operation = new DbOperation(_connectionString, _insertDml, _logger, parameter);

            return operation.ExecuteNonQueryAsync();
        }
    }
}
