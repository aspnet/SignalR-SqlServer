// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System.Data.Common;

namespace Microsoft.AspNetCore.SignalR.SqlServer
{
    internal static class DbParameterExtensions
    {
        public static DbParameter Clone(this DbParameter sourceParameter, IDbProviderFactory dbProviderFactory)
        {
            var newParameter = dbProviderFactory.CreateParameter();

            newParameter.ParameterName = sourceParameter.ParameterName;
            newParameter.DbType = sourceParameter.DbType;
            newParameter.Value = sourceParameter.Value;
            newParameter.Direction = sourceParameter.Direction;

            return newParameter;
        }
    }
}
