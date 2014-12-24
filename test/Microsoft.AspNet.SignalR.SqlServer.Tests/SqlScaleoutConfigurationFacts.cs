using System;
using Xunit;
using Xunit.Extensions;

namespace Microsoft.AspNet.SignalR.SqlServer.Tests 
{
    public class SqlScaleoutConfigurationFacts
    {
        [Theory]
        [InlineData(-1, false)]
        [InlineData(0, false)]
        [InlineData(1, true)]
        [InlineData(10, true)]
        public void TableCountValidated(int tableCount, bool isValid)
        {
            var config = new SqlScaleoutConfiguration();

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

        [Theory]
        [InlineData(null, false)]
        [InlineData("", false)]
        [InlineData("dummy", true)]
        public void ConnectionStringValidated(string connectionString, bool isValid)
        {
            var config = new SqlScaleoutConfiguration();
            if (isValid)
            {
                config.ConnectionString = connectionString;
            }
            else
            {
                Assert.Throws(typeof(ArgumentNullException), () => config.ConnectionString = connectionString);
            }
        }
    }
}
