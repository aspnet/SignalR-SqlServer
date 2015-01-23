// Copyright (c) Microsoft Open Technologies, Inc. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Threading;
using Microsoft.Framework.Logging;

namespace Microsoft.AspNet.SignalR.SqlServer
{
    /// <summary>
    /// A DbOperation that continues to execute over and over as new results arrive.
    /// Will attempt to use SQL Query Notifications, otherwise falls back to a polling receive loop.
    /// </summary>
    internal class ObservableDbOperation : DbOperation, IDisposable, IDbBehavior
    {
        private readonly Tuple<int, int>[] _updateLoopRetryDelays = new[] {
            Tuple.Create(0, 3),    // 0ms x 3
            Tuple.Create(10, 3),   // 10ms x 3
            Tuple.Create(50, 2),   // 50ms x 2
            Tuple.Create(100, 2),  // 100ms x 2
            Tuple.Create(200, 2),  // 200ms x 2
            Tuple.Create(1000, 2), // 1000ms x 2
            Tuple.Create(1500, 2), // 1500ms x 2
            Tuple.Create(3000, 1)  // 3000ms x 1
        };
        private readonly object _stopLocker = new object();
        private readonly ManualResetEventSlim _stopHandle = new ManualResetEventSlim(true);
        private readonly IDbBehavior _dbBehavior;

        private volatile bool _disposing;
        private long _notificationState;
        private readonly ILogger _logger;

        public ObservableDbOperation(string connectionString, string commandText, ILogger logger, IDbProviderFactory dbProviderFactory, IDbBehavior dbBehavior)
            : base(connectionString, commandText, logger, dbProviderFactory)
        {
            _dbBehavior = dbBehavior ?? this;
            _logger = logger;

            InitEvents();
        }

        public ObservableDbOperation(string connectionString, string commandText, ILogger logger, params DbParameter[] parameters)
            : base(connectionString, commandText, logger, parameters)
        {
            _dbBehavior = this;
            _logger = logger;

            InitEvents();
        }

        /// <summary>
        /// For use from tests only.
        /// </summary>
        internal long CurrentNotificationState
        {
            get { return _notificationState; }
            set { _notificationState = value; }
        }

        private void InitEvents()
        {
            Faulted += _ => { };
            Queried += () => { };
#if ASPNET50
            Changed += () => { };
#endif
        }

        public event Action Queried;
#if ASPNET50
        public event Action Changed;
#endif
        public event Action<Exception> Faulted;

        /// <summary>
        /// Note this blocks the calling thread until a SQL Query Notification can be set up
        /// </summary>
        [SuppressMessage("Microsoft.Maintainability", "CA1502:AvoidExcessiveComplexity", Justification = "Needs refactoring"),
         SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes", Justification = "Errors are reported via the callback")]
#if ASPNET50
        public void ExecuteReaderWithUpdates(Action<IDataRecord, DbOperation> processRecord)
#else
        public void ExecuteReaderWithUpdates(Action<DbDataReader, DbOperation> processRecord)
#endif
        {
            lock (_stopLocker)
            {
                if (_disposing)
                {
                    return;
                }
                _stopHandle.Reset();
            }

            var useNotifications = _dbBehavior.StartSqlDependencyListener();

            var delays = _dbBehavior.UpdateLoopRetryDelays;

            for (var i = 0; i < delays.Count; i++)
            {
                if (i == 0 && useNotifications)
                {
                    // Reset the state to ProcessingUpdates if this is the start of the loop.
                    // This should be safe to do here without Interlocked because the state is protected
                    // in the other two cases using Interlocked, i.e. there should only be one instance of
                    // this running at any point in time.
                    _notificationState = NotificationState.ProcessingUpdates;
                }

                Tuple<int, int> retry = delays[i];
                var retryDelay = retry.Item1;
                var retryCount = retry.Item2;

                for (var j = 0; j < retryCount; j++)
                {
                    if (_disposing)
                    {
                        Stop(null);
                        return;
                    }

                    if (retryDelay > 0)
                    {
                        Logger.WriteVerbose(String.Format("{0}Waiting {1}ms before checking for messages again", LoggerPrefix, retryDelay));

                        Thread.Sleep(retryDelay);
                    }

                    var recordCount = 0;
                    try
                    {
                        recordCount = ExecuteReader(processRecord);

                        Queried();
                    }
                    catch (Exception ex)
                    {
                        Logger.WriteError(String.Format("{0}Error in SQL receive loop: {1}", LoggerPrefix, ex));

                        Faulted(ex);
                    }

                    if (recordCount > 0)
                    {
                        Logger.WriteVerbose(String.Format("{0}{1} records received", LoggerPrefix, recordCount));

                        // We got records so start the retry loop again
                        i = -1;
                        break;
                    }

                    Logger.WriteVerbose("{0}No records received", LoggerPrefix);

                    var isLastRetry = i == delays.Count - 1 && j == retryCount - 1;

                    if (isLastRetry)
                    {
                        // Last retry loop iteration
                        if (!useNotifications)
                        {
                            // Last retry loop and we're not using notifications so just stay looping on the last retry delay
                            j = j - 1;
                        }
                        else
                        {
#if ASPNET50
                            // No records after all retries, set up a SQL notification
                            try
                            {
                                Logger.WriteVerbose("{0}Setting up SQL notification", LoggerPrefix);

                                recordCount = ExecuteReader(processRecord, command =>
                                {
                                    _dbBehavior.AddSqlDependency(command, e => SqlDependency_OnChange(e, processRecord));
                                });

                                Queried();

                                if (recordCount > 0)
                                {
                                    Logger.WriteVerbose("{0}Records were returned by the command that sets up the SQL notification, restarting the receive loop", LoggerPrefix);

                                    i = -1;
                                    break; // break the inner for loop
                                }
                                else
                                {
                                    var previousState = Interlocked.CompareExchange(ref _notificationState, NotificationState.AwaitingNotification,
                                        NotificationState.ProcessingUpdates);

                                    if (previousState == NotificationState.AwaitingNotification)
                                    {
                                        Logger.WriteError("{0}A SQL notification was already running. Overlapping receive loops detected, this should never happen. BUG!", LoggerPrefix);

                                        return;
                                    }

                                    if (previousState == NotificationState.NotificationReceived)
                                    {
                                        // Failed to change _notificationState from ProcessingUpdates to AwaitingNotification, it was already NotificationReceived

                                        Logger.WriteVerbose("{0}The SQL notification fired before the receive loop returned, restarting the receive loop", LoggerPrefix);

                                        i = -1;
                                        break; // break the inner for loop
                                    }

                                }

                                Logger.WriteVerbose("{0}No records received while setting up SQL notification", LoggerPrefix);

                                // We're in a wait state for a notification now so check if we're disposing
                                lock (_stopLocker)
                                {
                                    if (_disposing)
                                    {
                                        _stopHandle.Set();
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                Logger.WriteError(String.Format("{0}Error in SQL receive loop: {1}", LoggerPrefix, ex));
                                Faulted(ex);

                                // Re-enter the loop on the last retry delay
                                j = j - 1;

                                if (retryDelay > 0)
                                {
                                    Logger.WriteVerbose(String.Format("{0}Waiting {1}ms before checking for messages again", LoggerPrefix, retryDelay));

                                    Thread.Sleep(retryDelay);
                                }
                            }
#endif
                        }
                    }
                }
            }

            Logger.WriteVerbose("{0}Receive loop exiting", LoggerPrefix);
        }

        [SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes", Justification = "Disposing")]
        public void Dispose()
        {
            lock (_stopLocker)
            {
                _disposing = true;
            }

#if ASPNET50
            if (_notificationState != NotificationState.Disabled)
            {
                try
                {
                    SqlDependency.Stop(ConnectionString);
                }
                catch (Exception) { }
            }
#endif
            if (Interlocked.Read(ref _notificationState) == NotificationState.ProcessingUpdates)
            {
                _stopHandle.Wait();
            }
            _stopHandle.Dispose();
        }

#if ASPNET50
        protected virtual void AddSqlDependency(IDbCommand command, Action<SqlNotificationEventArgs> callback)
        {
            command.AddSqlDependency(e => callback(e));
        }

        [SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes", Justification = "On a background thread and we report exceptions asynchronously"),
         SuppressMessage("Microsoft.Usage", "CA1801:ReviewUnusedParameters", MessageId = "sender", Justification = "Event handler")]
        protected virtual void SqlDependency_OnChange(SqlNotificationEventArgs e, Action<DbDataReader, DbOperation> processRecord)
        {
            Logger.WriteInformation("{0}SQL notification change fired", LoggerPrefix);

            lock (_stopLocker)
            {
                if (_disposing)
                {
                    return;
                }
            }

            var previousState = Interlocked.CompareExchange(ref _notificationState,
                NotificationState.NotificationReceived, NotificationState.ProcessingUpdates);

            if (previousState == NotificationState.NotificationReceived)
            {
                Logger.WriteError("{0}Overlapping SQL change notifications received, this should never happen, BUG!", LoggerPrefix);

                return;
            }
            if (previousState == NotificationState.ProcessingUpdates)
            {
                // We're still in the original receive loop

                // New updates will be retreived by the original reader thread
                Logger.WriteVerbose("{0}Original reader processing is still in progress and will pick up the changes", LoggerPrefix);

                return;
            }

            // _notificationState wasn't ProcessingUpdates (likely AwaitingNotification)

            // Check notification args for issues
            if (e.Type == SqlNotificationType.Change)
            {
                if (e.Info == SqlNotificationInfo.Update)
                {
                    Logger.WriteVerbose(string.Format("{0}SQL notification details: Type={1}, Source={2}, Info={3}", LoggerPrefix, e.Type, e.Source, e.Info));
                }
                else if (e.Source == SqlNotificationSource.Timeout)
                {
                    Logger.WriteVerbose("{0}SQL notification timed out", LoggerPrefix);
                }
                else
                {
                    Logger.WriteError(string.Format("{0}Unexpected SQL notification details: Type={1}, Source={2}, Info={3}", LoggerPrefix, e.Type, e.Source, e.Info));

                    Faulted(new SqlMessageBusException(String.Format(CultureInfo.InvariantCulture, Resources.Error_UnexpectedSqlNotificationType, e.Type, e.Source, e.Info)));
                }
            }
            else if (e.Type == SqlNotificationType.Subscribe)
            {
                Debug.Assert(e.Info != SqlNotificationInfo.Invalid, "Ensure the SQL query meets the requirements for query notifications at http://msdn.microsoft.com/en-US/library/ms181122.aspx");

                Logger.WriteError(string.Format("{0}SQL notification subscription error: Type={1}, Source={2}, Info={3}", LoggerPrefix, e.Type, e.Source, e.Info));

                if (e.Info == SqlNotificationInfo.TemplateLimit)
                {
                    // We've hit a subscription limit, pause for a bit then start again
                    Thread.Sleep(2000);
                }
                else
                {
                    // Unknown subscription error, let's stop using query notifications
                    _notificationState = NotificationState.Disabled;
                    try
                    {
                        SqlDependency.Stop(ConnectionString);
                    }
                    catch (Exception) { }
                }
            }

            Changed();
        }      
#endif


        [SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes", Justification = "I need to")]
        protected virtual bool StartSqlDependencyListener()
        {
#if ASPNETCORE50
            return false;
#else
            lock (_stopLocker)
            {
                if (_disposing)
                {
                    return false;
                }
            }

            if (_notificationState == NotificationState.Disabled)
            {
                return false;
            }

            Logger.WriteVerbose("{0}Starting SQL notification listener", LoggerPrefix);
            try
            {
                if (SqlDependency.Start(ConnectionString))
                {
                    Logger.WriteVerbose("{0}SQL notification listener started", LoggerPrefix);
                }
                else
                {
                    Logger.WriteVerbose("{0}SQL notification listener was already running", LoggerPrefix);
                }
                return true;
            }
            catch (InvalidOperationException)
            {
                Logger.WriteInformation("{0}SQL Service Broker is disabled, disabling query notifications", LoggerPrefix);

                _notificationState = NotificationState.Disabled;
                return false;
            }
            catch (Exception ex)
            {
                Logger.WriteError(String.Format("{0}Error starting SQL notification listener: {1}", LoggerPrefix, ex));

                return false;
            }
#endif
        }

        [SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes", Justification = "Stopping is a terminal state on a bg thread")]
        protected virtual void Stop(Exception ex)
        {
            if (ex != null)
            {
                Faulted(ex);
            }

            if (_notificationState != NotificationState.Disabled)
            {
#if ASPNET50
                try
                {
                    Logger.WriteVerbose("{0}Stopping SQL notification listener", LoggerPrefix);
                    SqlDependency.Stop(ConnectionString);
                    Logger.WriteVerbose("{0}SQL notification listener stopped", LoggerPrefix);
                }
                catch (Exception stopEx)
                {
                    Logger.WriteError(String.Format("{0}Error occured while stopping SQL notification listener: {1}", LoggerPrefix, stopEx));
                }
#endif
            }

            lock (_stopLocker)
            {
                if (_disposing)
                {
                    _stopHandle.Set();
                }
            }
        }

        internal static class NotificationState
        {
            public const long Enabled = 0;
            public const long ProcessingUpdates = 1;
            public const long AwaitingNotification = 2;
            public const long NotificationReceived = 3;
            public const long Disabled = 4;
        }

        bool IDbBehavior.StartSqlDependencyListener()
        {
            return StartSqlDependencyListener();
        }

        IList<Tuple<int, int>> IDbBehavior.UpdateLoopRetryDelays
        {
            get { return _updateLoopRetryDelays; }
        }

#if ASPNET50
        void IDbBehavior.AddSqlDependency(IDbCommand command, Action<SqlNotificationEventArgs> callback)
        {
            AddSqlDependency(command, callback);
        }
#endif
    }
}
