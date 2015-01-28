// Copyright (c) Microsoft Open Technologies, Inc. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using Microsoft.AspNet.SignalR;
using Microsoft.AspNet.SignalR.Messaging;
using Microsoft.AspNet.SignalR.SqlServer;
using Microsoft.Framework.ConfigurationModel;

namespace Microsoft.Framework.DependencyInjection
{
    public static class SqlServerSignalRServicesBuilderExtensions
    {
        public static SignalRServicesBuilder AddSqlServer(this SignalRServicesBuilder builder, Action<SqlScaleoutConfiguration> configureOptions = null)
        {
            return builder.AddSqlServer(configuration: null, configureOptions: configureOptions);
        }
        public static SignalRServicesBuilder AddSqlServer(this SignalRServicesBuilder builder, IConfiguration configuration, Action<SqlScaleoutConfiguration> configureOptions = null)
        {
            var describe = new ServiceDescriber(configuration);

            builder.ServiceCollection.Add(describe.Singleton<IMessageBus, SqlMessageBus>());

            if (configuration != null)
            {
                builder.ServiceCollection.Configure<SqlScaleoutConfiguration>(configuration);
            }

            if (configureOptions != null)
            {
                builder.ServiceCollection.Configure(configureOptions);
            }

            return builder;
        }
    }
}
