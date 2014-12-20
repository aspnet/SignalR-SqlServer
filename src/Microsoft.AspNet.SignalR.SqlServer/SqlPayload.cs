// Copyright (c) Microsoft Open Technologies, Inc. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using Microsoft.AspNet.SignalR.Messaging;
using System.Data.Common;

namespace Microsoft.AspNet.SignalR.SqlServer
{
    public static class SqlPayload
    {
        public static byte[] ToBytes(IList<Message> messages)
        {
            if (messages == null)
            {
                throw new ArgumentNullException("messages");
            }

            var message = new ScaleoutMessage(messages);
            return message.ToBytes();
        }

        public static ScaleoutMessage FromBytes(DbDataReader record)
        {
            var message = ScaleoutMessage.FromBytes(record.GetBinary(1));

            return message;
        }
    }
}
