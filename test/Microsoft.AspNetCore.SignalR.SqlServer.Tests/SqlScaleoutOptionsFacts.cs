// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using Xunit;

namespace Microsoft.AspNet.SignalR.SqlServer.Tests
{
    public class SqlScaleoutConfigurationFacts
    {
        [Theory]
        [InlineData(null, false)]
        [InlineData("", false)]
        [InlineData("dummy", true)]
        public void ConnectionStringValidated(string connectionString, bool isValid)
        {
            var config = new SqlScaleoutOptions();
            if (isValid)
            {
                config.ConnectionString = connectionString;
            }
            else
            {
                Assert.Throws(typeof(ArgumentNullException), () => config.ConnectionString = connectionString);
            }
        }

        [Theory]
        [InlineData(-1, false)]
        [InlineData(0, false)]
        [InlineData(1, true)]
        [InlineData(10, true)]
        public void TableCountValidated(int tableCount, bool isValid)
        {
            var config = new SqlScaleoutOptions();

            if (isValid)
            {
                config.ConnectionString = "dummy";
                config.TableCount = tableCount;
            }
            else
            {
                Assert.Throws(typeof(ArgumentOutOfRangeException), () => config.TableCount = tableCount);
            }
        }
    }
}
