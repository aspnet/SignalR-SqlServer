// Copyright (c) Microsoft Open Technologies, Inc. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using Microsoft.AspNet.SignalR.Messaging;

namespace Microsoft.AspNet.SignalR
{
    /// <summary>
    /// Settings for the SQL Server scale-out message bus implementation.
    /// </summary>
    public class SqlScaleoutConfiguration : ScaleoutConfiguration
    {
        private string _connectionString;
        private int _tableCount;
        
        public SqlScaleoutConfiguration()
        {
            _tableCount = 1;
        }
        /// <summary>
        /// The SQL Server connection string to use.
        /// </summary>
        public string ConnectionString
        {
            get
            {
                return _connectionString;
            }
            set
            {
                if (String.IsNullOrEmpty(value))
                {
                    throw new ArgumentNullException("connectionString");
                }

                _connectionString = value;
            }
        }

        /// <summary>
        /// The number of tables to store messages in. Using more tables reduces lock contention and may increase throughput.
        /// This must be consistent between all nodes in the web farm.
        /// Defaults to 1.
        /// </summary>
        public int TableCount
        {
            get
            {
                return _tableCount;
            }
            set
            {
                if (value < 1)
                {
                    throw new ArgumentOutOfRangeException("value");
                }
                _tableCount = value;
            }
        }
    }
}
