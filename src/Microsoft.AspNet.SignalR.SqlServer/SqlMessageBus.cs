// Copyright (c) Microsoft Open Technologies, Inc. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNet.SignalR.Messaging;
using Microsoft.Framework.DependencyInjection;
using Microsoft.AspNet.SignalR.Infrastructure;
using Microsoft.Framework.Logging;
using Microsoft.Framework.OptionsModel;

namespace Microsoft.AspNet.SignalR.SqlServer
{
    /// <summary>
    /// Uses SQL Server tables to scale-out SignalR applications in web farms.
    /// </summary>
    public class SqlMessageBus : ScaleoutMessageBus
    {
        internal const string SchemaName = "SignalR";

        private const string _tableNamePrefix = "Messages";

        private readonly string _connectionString;
        private readonly SqlScaleoutConfiguration _configuration;
       
        private readonly ILogger _logger;
        private readonly IDbProviderFactory _dbProviderFactory;
        private readonly List<SqlStream> _streams = new List<SqlStream>();

        /// <summary>
        /// Creates a new instance of the SqlMessageBus class.
        /// </summary>
        /// <param name="resolver">The resolver to use.</param>
        /// <param name="configuration">The SQL scale-out configuration options.</param>
        public SqlMessageBus(IStringMinifier stringMinifier,
                                     ILoggerFactory loggerFactory,
                                     IPerformanceCounterManager performanceCounterManager,
                                     IOptions<SignalROptions> optionsAccessor,
                                     IOptions<SqlScaleoutConfiguration> scaleoutConfigurationAccessor)
            : this(stringMinifier, loggerFactory, performanceCounterManager, optionsAccessor, scaleoutConfigurationAccessor, SqlClientFactory.Instance.AsIDbProviderFactory())
        {

        }

        internal SqlMessageBus(IStringMinifier stringMinifier,
                                     ILoggerFactory loggerFactory,
                                     IPerformanceCounterManager performanceCounterManager,
                                     IOptions<SignalROptions> optionsAccessor,
                                     IOptions<SqlScaleoutConfiguration> scaleoutConfigurationAccessor,
                                     IDbProviderFactory dbProviderFactory)
            : base(stringMinifier, loggerFactory, performanceCounterManager, optionsAccessor, scaleoutConfigurationAccessor)
        {
            var configuration = scaleoutConfigurationAccessor.Options;
            _connectionString = configuration.ConnectionString;
            _configuration = configuration;
            _dbProviderFactory = dbProviderFactory;
            
            _logger = loggerFactory.Create("SignalR." + typeof(SqlMessageBus).Name);
            ThreadPool.QueueUserWorkItem(Initialize);
        }

        protected override int StreamCount
        {
            get
            {
                return _configuration.TableCount;
            }
        }

        protected override Task Send(int streamIndex, IList<Message> messages)
        {
            return _streams[streamIndex].Send(messages);
        }

        protected override void Dispose(bool disposing)
        {
            _logger.WriteInformation("SQL message bus disposing, disposing streams");

            for (var i = 0; i < _streams.Count; i++)
            {
                _streams[i].Dispose();
            }

            base.Dispose(disposing);
        }

        [SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope", Justification = "They're stored in a List and disposed in the Dispose method"),
         SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes", Justification = "On a background thread and we report exceptions asynchronously")]
        private void Initialize(object state)
        {
            // NOTE: Called from a ThreadPool thread
            _logger.WriteInformation(String.Format("SQL message bus initializing, TableCount={0}", _configuration.TableCount));

            while (true)
            {
                try
                {
                    var installer = new SqlInstaller(_connectionString, _tableNamePrefix, _configuration.TableCount, _logger);
                    installer.Install();
                    break;
                }
                catch (Exception ex)
                {
                    // Exception while installing
                    for (var i = 0; i < _configuration.TableCount; i++)
                    {
                        OnError(i, ex);
                    }

                     _logger.WriteError("Error trying to install SQL server objects, trying again in 2 seconds: {0}", ex);

                    // Try again in a little bit
                    Thread.Sleep(2000);
                }
            }

            for (var i = 0; i < _configuration.TableCount; i++)
            {
                var streamIndex = i;
                var tableName = String.Format(CultureInfo.InvariantCulture, "{0}_{1}", _tableNamePrefix, streamIndex);

                var stream = new SqlStream(streamIndex, _connectionString, tableName, _logger, _dbProviderFactory);
                stream.Queried += () => Open(streamIndex);
                stream.Faulted += (ex) => OnError(streamIndex, ex);
                stream.Received += (id, messages) => OnReceived(streamIndex, id, messages);

                _streams.Add(stream);

                StartReceiving(streamIndex);
            }
        }

        private void StartReceiving(int streamIndex)
        {
            var stream = _streams[streamIndex];
            stream.StartReceiving()
                // Open the stream once receiving has started
                .Then(() => Open(streamIndex))
                // Starting the receive loop failed
                .Catch(ex =>
                {
                    OnError(streamIndex, ex);

                    // Try again in a little bit
                    Thread.Sleep(2000);
                    StartReceiving(streamIndex);
                },
                _logger);
        }
    }
}
