using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data.OleDb;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using Serilog;

namespace AttendanceSyncService
{
    public class Program
    {
        public static void Main(string[] args)
        {
            // Configure Serilog for bootstrapping to capture startup errors.
            Log.Logger = new LoggerConfiguration()
                .Enrich.FromLogContext()
                .WriteTo.Console()
                .CreateBootstrapLogger();

            Log.Information("Starting up service host...");

            try
            {
                CreateHostBuilder(args).Build().Run();
                Log.Information("Service host shut down cleanly.");
            }
            catch (Exception ex)
            {
                Log.Fatal(ex, "An unhandled exception occurred during host startup.");
            }
            finally
            {
                Log.CloseAndFlush();
            }
        }

        public static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
                .UseWindowsService() // Configure the app to run as a Windows Service.
                .UseSerilog((context, services, configuration) => configuration
                    .ReadFrom.Configuration(context.Configuration)
                    .ReadFrom.Services(services))
                .ConfigureServices((context, services) =>
                {
                    // Register the main data syncer logic as a Singleton.
                    services.AddSingleton<AttendanceDataSyncer>();
                    // Register the SyncWorker as the background service.
                    services.AddHostedService<SyncWorker>();
                });
    }

    /// <summary>
    /// A background service that periodically triggers the data synchronization process.
    /// </summary>
    public class SyncWorker : BackgroundService
    {
        private readonly ILogger<SyncWorker> _logger;
        private readonly AttendanceDataSyncer _dataSyncer;
        private readonly TimeSpan _syncInterval;

        public SyncWorker(ILogger<SyncWorker> logger, AttendanceDataSyncer dataSyncer, IConfiguration configuration)
        {
            _logger = logger;
            _dataSyncer = dataSyncer;
            var intervalMinutes = configuration.GetValue<int>("AppSettings:SyncIntervalMinutes", 1);
            _syncInterval = TimeSpan.FromMinutes(intervalMinutes > 0 ? intervalMinutes : 1);
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("Attendance Sync Service starting.");
            _logger.LogInformation("Sync interval set to {Minutes} minutes.", _syncInterval.TotalMinutes);

            // Initial delay to allow other services to start up.
            await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);

            while (!stoppingToken.IsCancellationRequested)
            {
                await _dataSyncer.SyncDataAsync();
                _logger.LogInformation("Next sync scheduled in {SyncInterval}", _syncInterval);

                try
                {
                    await Task.Delay(_syncInterval, stoppingToken);
                }
                catch (TaskCanceledException)
                {
                    // This exception is expected when the service is stopping.
                    break;
                }
            }
        }

        public override async Task StopAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Attendance Sync Service is stopping.");
            await base.StopAsync(cancellationToken);
            _logger.LogInformation("Attendance Sync Service has stopped.");
        }
    }

    /// <summary>
    /// Contains the core logic for reading data from Access, processing it, and writing to Supabase.
    /// </summary>
    public class AttendanceDataSyncer
    {
        private readonly ILogger<AttendanceDataSyncer> _logger;
        private readonly string _accessPath;
        private readonly string _supabaseConnectionString;
        private readonly string _lastSyncFile;
        private readonly TimeZoneInfo _sourceTimeZone;

        public AttendanceDataSyncer(ILogger<AttendanceDataSyncer> logger, IConfiguration configuration)
        {
            _logger = logger;
            _accessPath = configuration["AppSettings:AccessDatabasePath"]
                ?? throw new InvalidOperationException("AppSettings:AccessDatabasePath is not configured.");
            _supabaseConnectionString = configuration.GetConnectionString("SupabaseConnection")
                ?? throw new InvalidOperationException("ConnectionStrings:SupabaseConnection is not configured.");
            _lastSyncFile = configuration["AppSettings:LastSyncFilePath"]
                ?? Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.CommonApplicationData), "AttendanceSyncService", "lastSync.txt");

            // Load the source timezone from config, defaulting to UTC if not found.
            try
            {
                var timeZoneId = configuration["AppSettings:SourceTimeZoneId"] ?? "UTC";
                _sourceTimeZone = TimeZoneInfo.FindSystemTimeZoneById(timeZoneId);
                _logger.LogInformation("Source timezone set to: {TimeZoneId}", _sourceTimeZone.Id);
            }
            catch (TimeZoneNotFoundException)
            {
                _logger.LogError("The configured timezone ID was not found. Defaulting to UTC.");
                _sourceTimeZone = TimeZoneInfo.Utc;
            }

            var lastSyncDir = Path.GetDirectoryName(_lastSyncFile);
            if (!string.IsNullOrEmpty(lastSyncDir))
            {
                Directory.CreateDirectory(lastSyncDir);
            }
        }

        public async Task SyncDataAsync()
        {
            try
            {
                _logger.LogInformation("--- Sync cycle started at {Time} ---", DateTimeOffset.Now);

                DateTimeOffset lastSyncDate = GetLastSyncDate();
                DateTimeOffset currentSyncDate = DateTimeOffset.Now;

                _logger.LogInformation("Syncing records with DownloadDate between {LastSyncDate} and {CurrentSyncDate}", lastSyncDate, currentSyncDate);

                var attendanceLogs = await GetAttendanceLogsFromAccessAsync(lastSyncDate, currentSyncDate);

                if (!attendanceLogs.Any())
                {
                    _logger.LogInformation("No new records found in the source database.");
                }
                else
                {
                    _logger.LogInformation("Retrieved {RecordCount} raw records from Access database.", attendanceLogs.Count);

                    var filteredLogs = FilterDuplicatePunches(attendanceLogs);
                    _logger.LogInformation("{FilteredCount} records remaining after deduplication.", filteredLogs.Count);

                    if (filteredLogs.Any())
                    {
                        await InsertIntoSupabaseAsync(filteredLogs);
                        _logger.LogInformation("Successfully synced {RecordCount} records to Supabase.", filteredLogs.Count);
                    }
                }

                SaveLastSyncDate(currentSyncDate);
                _logger.LogInformation("--- Sync cycle completed successfully. ---");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "An error occurred during the sync cycle. Last sync date will not be updated to retry this window.");
            }
        }

        private async Task<List<AttendanceLog>> GetAttendanceLogsFromAccessAsync(DateTimeOffset lastSyncDate, DateTimeOffset currentSyncDate)
        {
            var logs = new List<AttendanceLog>();
            string connectionString = $"Provider=Microsoft.ACE.OLEDB.12.0;Data Source={_accessPath};";

            // Convert the DateTimeOffset to the local time of the source database for the query.
            DateTime accessQueryStartDate = TimeZoneInfo.ConvertTime(lastSyncDate, _sourceTimeZone).DateTime;
            DateTime accessQueryEndDate = TimeZoneInfo.ConvertTime(currentSyncDate, _sourceTimeZone).DateTime;

            string formattedLastSync = accessQueryStartDate.ToString("MM/dd/yyyy HH:mm:ss", CultureInfo.InvariantCulture);
            string formattedCurrentSync = accessQueryEndDate.ToString("MM/dd/yyyy HH:mm:ss", CultureInfo.InvariantCulture);

            await using var connection = new OleDbConnection(connectionString);
            await connection.OpenAsync();

            var tablesToQuery = GetTableNamesForDateRange(accessQueryStartDate, accessQueryEndDate);
            _logger.LogInformation("Querying tables: {Tables}", string.Join(", ", tablesToQuery));

            foreach (var tableName in tablesToQuery)
            {
                try
                {
                    string query = $@"
                        SELECT [UserId] AS eID, [LogDate] AS evtTime, [DownloadDate] AS dlDate
                        FROM [{tableName}]
                        WHERE [DownloadDate] > #{formattedLastSync}# AND [DownloadDate] <= #{formattedCurrentSync}#";

                    await using var command = new OleDbCommand(query, connection);
                    await using var reader = await command.ExecuteReaderAsync();

                    while (await reader.ReadAsync())
                    {
                        var employeeId = reader["eID"]?.ToString();
                        if (!string.IsNullOrEmpty(employeeId) && reader["evtTime"] != DBNull.Value && reader["dlDate"] != DBNull.Value)
                        {
                            // DateTime from Access has Kind=Unspecified. Treat it as a wall-clock time in the source timezone.
                            var eventTimeUnspecified = (DateTime)reader["evtTime"];
                            var downloadDateUnspecified = (DateTime)reader["dlDate"];

                            // Convert to a timezone-aware DateTimeOffset.
                            var eventTime = new DateTimeOffset(eventTimeUnspecified, _sourceTimeZone.GetUtcOffset(eventTimeUnspecified));
                            var downloadDate = new DateTimeOffset(downloadDateUnspecified, _sourceTimeZone.GetUtcOffset(downloadDateUnspecified));

                            logs.Add(new AttendanceLog
                            {
                                EmployeeID = employeeId,
                                EventTime = eventTime,
                                DownloadDate = downloadDate
                            });
                        }
                    }
                }
                catch (OleDbException ex) when (ex.Message.Contains("cannot find", StringComparison.OrdinalIgnoreCase))
                {
                    _logger.LogDebug("Table {TableName} not found, skipping.", tableName);
                }
            }
            return logs;
        }

        private List<AttendanceLog> FilterDuplicatePunches(List<AttendanceLog> logs)
        {
            if (logs.Count < 2) return logs;

            var filteredList = new List<AttendanceLog>();
            // Group by employee and sort their punches by time to process them chronologically.
            var groupedLogs = logs.GroupBy(l => l.EmployeeID)
                                  .Select(g => g.OrderBy(l => l.EventTime).ToList());

            foreach (var employeePunches in groupedLogs)
            {
                if (!employeePunches.Any()) continue;

                // Add the very first punch for the employee.
                filteredList.Add(employeePunches[0]);
                var lastAddedPunchTime = employeePunches[0].EventTime;

                // Iterate through the rest of the punches.
                for (int i = 1; i < employeePunches.Count; i++)
                {
                    var currentPunch = employeePunches[i];
                    // Only add the punch if it's more than 2 minutes after the last added one.
                    if ((currentPunch.EventTime - lastAddedPunchTime).TotalMinutes > 2)
                    {
                        filteredList.Add(currentPunch);
                        lastAddedPunchTime = currentPunch.EventTime;
                    }
                }
            }

            // Return the list sorted by the original download date for sequential insertion.
            return filteredList.OrderBy(l => l.DownloadDate).ToList();
        }

        private List<string> GetTableNamesForDateRange(DateTime startDate, DateTime endDate)
        {
            var tableNames = new HashSet<string>();
            var loopDate = new DateTime(startDate.Year, startDate.Month, 1);
            var finalDate = new DateTime(endDate.Year, endDate.Month, 1);

            while (loopDate <= finalDate)
            {
                tableNames.Add($"DeviceLogs_{loopDate.Month}_{loopDate.Year}");
                loopDate = loopDate.AddMonths(1);
            }
            return tableNames.ToList();
        }

        private async Task InsertIntoSupabaseAsync(List<AttendanceLog> logs)
        {
            await using var connection = new NpgsqlConnection(_supabaseConnectionString);
            await connection.OpenAsync();

            // The COPY command is the most performant way to bulk insert data into PostgreSQL.
            await using var writer = await connection.BeginBinaryImportAsync(
                "COPY attendance_logs (employee_id, event_time, download_date) FROM STDIN (FORMAT BINARY)");

            foreach (var log in logs)
            {
                await writer.StartRowAsync();
                await writer.WriteAsync(log.EmployeeID, NpgsqlDbType.Varchar);
                // **FIX**: Convert to UTC before writing for the binary importer.
                await writer.WriteAsync(log.EventTime.ToUniversalTime(), NpgsqlDbType.TimestampTz);
                await writer.WriteAsync(log.DownloadDate.ToUniversalTime(), NpgsqlDbType.TimestampTz);
            }

            await writer.CompleteAsync();
        }

        private DateTimeOffset GetLastSyncDate()
        {
            try
            {
                if (File.Exists(_lastSyncFile))
                {
                    string content = File.ReadAllText(_lastSyncFile).Trim();
                    // Use DateTimeOffset.Parse with RoundtripKind style for ISO 8601 format.
                    if (DateTimeOffset.TryParse(content, null, DateTimeStyles.RoundtripKind, out var lastSync))
                    {
                        _logger.LogInformation("Found last sync date: {LastSync}", lastSync);
                        return lastSync;
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Could not read last sync date from file. Defaulting to the last 7 days.");
            }

            // If the file doesn't exist or is invalid, sync the last 7 days.
            _logger.LogInformation("No last sync file found. Defaulting to sync the last 7 days.");
            return DateTimeOffset.Now.AddDays(-7);
        }

        private void SaveLastSyncDate(DateTimeOffset syncDate)
        {
            try
            {
                // The "o" format specifier (e.g., 2025-10-22T10:03:48.1234567+02:00) is standard and includes timezone info.
                File.WriteAllText(_lastSyncFile, syncDate.ToString("o", CultureInfo.InvariantCulture));
                _logger.LogInformation("Last sync date saved as: {SyncDate}", syncDate);
            }
            catch (Exception ex)
            {
                _logger.LogCritical(ex, "FATAL: Could not save the last sync date. This will cause re-processing of data on next run.");
            }
        }
    }

    /// <summary>
    /// Represents a single attendance log record with timezone awareness.
    /// </summary>
    public class AttendanceLog
    {
        public required string EmployeeID { get; set; }
        public DateTimeOffset EventTime { get; set; }
        public DateTimeOffset DownloadDate { get; set; }
    }
}