// Copyright (c) Microsoft Open Technologies, Inc. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;

namespace Microsoft.AspNet.SignalR.SqlServer
{
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1032:ImplementStandardExceptionConstructors", Justification="Should never have inner exceptions")]
#if DNX451
    [Serializable]
#endif
    public class SqlMessageBusException : Exception
    {
        public SqlMessageBusException(string message)
            : base(message)
        {

        }
    }
}
