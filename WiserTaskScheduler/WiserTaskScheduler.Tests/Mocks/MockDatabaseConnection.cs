using System.Data;
using System.Data.Common;
using GeeksCoreLibrary.Core.Models;
using GeeksCoreLibrary.Modules.Databases.Interfaces;

namespace WiserTaskScheduler.Tests.Mocks;

public class MockDatabaseConnection : IDatabaseConnection
{
    private Dictionary<string, object> parameters = new Dictionary<string, object>();

    public async ValueTask DisposeAsync()
    {
        throw new NotImplementedException();
    }

    public void Dispose()
    {

    }

    public async Task<DbDataReader> GetReaderAsync(string query)
    {
        throw new NotImplementedException();
    }

    public async Task<T> ExecuteScalarAsync<T>(string query, bool skipCache = false, bool cleanUp = true, bool useWritingConnectionIfAvailable = false)
    {
        throw new NotImplementedException();
    }

    public async Task<object> ExecuteScalarAsync(string query, bool skipCache = false, bool cleanUp = true, bool useWritingConnectionIfAvailable = false)
    {
        throw new NotImplementedException();
    }

    public Task<DataTable> GetAsync(string query, bool skipCache = false, bool cleanUp = true, bool useWritingConnectionIfAvailable = false)
    {
        var dataTable = new DataTable();

        if (query.StartsWith("QueriesService1"))
        {
            dataTable.Columns.Add("testValue", typeof(string));
            var row = dataTable.NewRow();
            row["testValue"] = 1;
            dataTable.Rows.Add(row);
        }
        else if (query.StartsWith("#QueriesService2:"))
        {
            dataTable.Columns.Add("returnValue", typeof(string));
            var row = dataTable.NewRow();
            row["returnValue"] = parameters.Single(x => x.Key.StartsWith("MyValuewts")).Value;
            dataTable.Rows.Add(row);
        }

        return Task.FromResult(dataTable);
    }

    public async Task<string> GetAsJsonAsync(string query, bool formatResult = false, bool skipCache = false)
    {
        throw new NotImplementedException();
    }

    public async Task<int> ExecuteAsync(string query, bool useWritingConnectionIfAvailable = true, bool cleanUp = true)
    {
        throw new NotImplementedException();
    }

    public async Task<T> InsertOrUpdateRecordBasedOnParametersAsync<T>(string tableName, T id = default(T), string idColumnName = "id", bool ignoreErrors = false, bool useWritingConnectionIfAvailable = true)
    {
        throw new NotImplementedException();
    }

    public async Task<long> InsertRecordAsync(string query, bool useWritingConnectionIfAvailable = true)
    {
        throw new NotImplementedException();
    }

    public async Task<IDbTransaction> BeginTransactionAsync(bool forceNewTransaction = false)
    {
        throw new NotImplementedException();
    }

    public async Task CommitTransactionAsync(bool throwErrorIfNoActiveTransaction = true)
    {
        throw new NotImplementedException();
    }

    public async Task RollbackTransactionAsync(bool throwErrorIfNoActiveTransaction = true)
    {
        throw new NotImplementedException();
    }

    public void AddParameter(string key, object value)
    {
        parameters.Add(key, value);
    }

    public void ClearParameters()
    {
        parameters.Clear();
    }

    public string GetDatabaseNameForCaching(bool writeDatabase = false)
    {
        throw new NotImplementedException();
    }

    public async Task EnsureOpenConnectionForReadingAsync()
    {

    }

    public async Task EnsureOpenConnectionForWritingAsync()
    {

    }

    public async Task ChangeConnectionStringsAsync(string newConnectionStringForReading, string newConnectionStringForWriting = null, SshSettings sshSettingsForReading = null, SshSettings sshSettingsForWriting = null)
    {
        await Task.CompletedTask;
    }

    public void SetCommandTimeout(int value)
    {

    }

    public bool HasActiveTransaction()
    {
        throw new NotImplementedException();
    }

    public DbConnection GetConnectionForReading()
    {
        throw new NotImplementedException();
    }

    public DbConnection GetConnectionForWriting()
    {
        throw new NotImplementedException();
    }

    public async Task<int> BulkInsertAsync(DataTable dataTable, string tableName, bool useWritingConnectionIfAvailable = true, bool useInsertIgnore = false)
    {
        throw new NotImplementedException();
    }

    public string ConnectedDatabase { get; }
    public string ConnectedDatabaseForWriting { get; }
}