using System;

namespace JetBrains.Annotations
{
    [AttributeUsage(AttributeTargets.Parameter, AllowMultiple = false)]
    internal sealed class NotNullAttribute : Attribute
    {
    }
}