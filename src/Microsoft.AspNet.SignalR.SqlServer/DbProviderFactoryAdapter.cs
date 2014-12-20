// Copyright (c) Microsoft Open Technologies, Inc. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System.Data.Common;

namespace Microsoft.AspNet.SignalR.SqlServer
{
    internal class DbProviderFactoryAdapter : IDbProviderFactory
    {
        private readonly DbProviderFactory _dbProviderFactory;

        public DbProviderFactoryAdapter(DbProviderFactory dbProviderFactory)
        {
            _dbProviderFactory = dbProviderFactory;
        }

        public DbConnection CreateConnection()
        {
            return _dbProviderFactory.CreateConnection();
        }

        public DbParameter CreateParameter()
        {
            return _dbProviderFactory.CreateParameter();
        }
    }
}
