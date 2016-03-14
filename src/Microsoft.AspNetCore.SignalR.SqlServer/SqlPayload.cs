// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using JetBrains.Annotations;
using Microsoft.AspNetCore.SignalR.Messaging;

namespace Microsoft.AspNetCore.SignalR.SqlServer
{
    public static class SqlPayload
    {
        public static byte[] ToBytes([NotNull] IList<Message> messages)
        {
            var message = new ScaleoutMessage(messages);
            return message.ToBytes();
        }

#if NET451
        public static ScaleoutMessage FromBytes(IDataRecord record)
#else
        public static ScaleoutMessage FromBytes(DbDataReader record)
#endif
        {
            var message = ScaleoutMessage.FromBytes(record.GetBinary(1));

            return message;
        }
    }
}
