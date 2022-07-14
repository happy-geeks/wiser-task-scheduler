using System;

namespace AutoImportServiceCore.Core.Models;

public class ResultSetException : Exception
{
    public ResultSetException() {}

    public ResultSetException(string message) : base(message) {}
    
    public ResultSetException(string message, Exception inner) : base (message, inner) {}
}