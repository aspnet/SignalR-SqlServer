// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using Microsoft.AspNet.SignalR;
using Microsoft.AspNet.SignalR.Messaging;
using Microsoft.AspNet.SignalR.SqlServer;
using Microsoft.Extensions.Configuration;

namespace Microsoft.Extensions.DependencyInjection
{
    public static class SqlServerSignalRServicesBuilderExtensions
    {
        public static SignalRServicesBuilder AddSqlServer(this SignalRServicesBuilder builder, Action<SqlScaleoutOptions> configureOptions = null)
        {
            return builder.AddSqlServer(configuration: null, configureOptions: configureOptions);
        }
        public static SignalRServicesBuilder AddSqlServer(this SignalRServicesBuilder builder, IConfiguration configuration, Action<SqlScaleoutOptions> configureOptions = null)
        {
            builder.ServiceCollection.Add(ServiceDescriptor.Singleton<IMessageBus, SqlMessageBus>());

            if (configuration != null)
            {
                builder.ServiceCollection.Configure<SqlScaleoutOptions>(configuration);
            }

            if (configureOptions != null)
            {
                builder.ServiceCollection.Configure(configureOptions);
            }

            return builder;
        }
    }
}
