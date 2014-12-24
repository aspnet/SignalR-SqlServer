// Copyright (c) Microsoft Open Technologies, Inc. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlClient;

namespace Microsoft.AspNet.SignalR.SqlServer
{
    public interface IDbBehavior
    {
        bool StartSqlNotificationListener();
        IList<Tuple<int, int>> UpdateLoopRetryDelays { get; }

#if ASPNET50
        void AddSqlDependency(DbCommand command, Action<SqlNotificationEventArgs> callback);
#else
        
        void AddSqlNotification(DbCommand command, Action callback);

#endif
    }
}
