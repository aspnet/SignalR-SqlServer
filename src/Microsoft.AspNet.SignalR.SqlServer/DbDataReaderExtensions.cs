// Copyright (c) Microsoft Open Technologies, Inc. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Data.Common;
using System.Data.SqlClient;
using JetBrains.Annotations;

namespace Microsoft.AspNet.SignalR.SqlServer
{
    internal static class DbDataReaderExtensions
    {
        public static byte[] GetBinary([NotNull]this DbDataReader reader, int ordinalIndex)
        {
            var sqlReader = reader as SqlDataReader;
            if (sqlReader == null)
            {
                throw new NotSupportedException();
            }

            return sqlReader.GetSqlBinary(ordinalIndex).Value;
        }
    }
}
