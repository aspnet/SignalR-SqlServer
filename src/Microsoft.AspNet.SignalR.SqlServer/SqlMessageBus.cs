// Copyright (c) Microsoft Open Technologies, Inc. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNet.SignalR.Messaging;
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
        private readonly SqlScaleoutOptions _configuration;

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
                                     IOptions<MessageBusOptions> optionsAccessor,
                                     IOptions<SqlScaleoutOptions> scaleoutOptionsAccessor)
            : this(stringMinifier, loggerFactory, performanceCounterManager, optionsAccessor, scaleoutOptionsAccessor, SqlClientFactory.Instance.AsIDbProviderFactory())
        {

        }

        internal SqlMessageBus(IStringMinifier stringMinifier,
                                     ILoggerFactory loggerFactory,
                                     IPerformanceCounterManager performanceCounterManager,
                                     IOptions<MessageBusOptions> optionsAccessor,
                                     IOptions<SqlScaleoutOptions> scaleoutOptionsAccessor,
                                     IDbProviderFactory dbProviderFactory)
            : base(stringMinifier, loggerFactory, performanceCounterManager, optionsAccessor, scaleoutOptionsAccessor)
        {
            var configuration = scaleoutOptionsAccessor.Options;
            _connectionString = configuration.ConnectionString;
            _configuration = configuration;
            _dbProviderFactory = dbProviderFactory;

            _logger = loggerFactory.CreateLogger<SqlMessageBus>();
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
            _logger.LogInformation("SQL message bus disposing, disposing streams");

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
            _logger.LogInformation(String.Format("SQL message bus initializing, TableCount={0}", _configuration.TableCount));

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

                    _logger.LogError("Error trying to install SQL server objects, trying again in 2 seconds: {0}", ex);

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

            stream.StartReceiving().ContinueWith(async task =>
            {
                try
                {
                    await task;
                    // Open the stream once receiving has started
                    Open(streamIndex);
                }
                catch (Exception ex)
                {
                    OnError(streamIndex, ex);

                    _logger.LogWarning("Exception thrown by Task", ex);
                    Thread.Sleep(2000);
                    StartReceiving(streamIndex);
                }
            });
        }
    }
}
