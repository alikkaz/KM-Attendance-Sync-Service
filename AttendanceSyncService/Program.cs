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
            // Configure Serilog for bootstrapping. This captures any errors that happen during host startup.
            Log.Logger = new LoggerConfiguration()
                .WriteTo.Console()
                .CreateBootstrapLogger();

            Log.Information("Starting up the service host...");

            try
            {
                CreateHostBuilder(args).Build().Run();
            }
            catch (Exception ex)
            {
                Log.Fatal(ex, "An unhandled exception occurred during host startup");
            }
            finally
            {
                // Ensure all log messages are flushed to their destinations before the application closes.
                Log.CloseAndFlush();
            }
        }

        public static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
                // Configures the application to be run as a Windows Service.
                .UseWindowsService()
                // Integrates Serilog for advanced logging, reading configuration from appsettings.json.
                .UseSerilog((context, services, configuration) => configuration
                    .ReadFrom.Configuration(context.Configuration)
                    .ReadFrom.Services(services)
                    .Enrich.FromLogContext()
                    .WriteTo.Console()
                )
                // Configures the application's services for dependency injection.
                .ConfigureServices((context, services) =>
                {
                    // Register the main data syncer logic as a Singleton. It's safe because it's stateless.
                    services.AddSingleton<AttendanceDataSyncer>();
                    // Register the SyncWorker as a Hosted Service, which will be started and stopped by the host.
                    services.AddHostedService<SyncWorker>();
                });
    }

    /// <summary>
    /// A background service that periodically triggers the data synchronization process.
    /// This class is responsible for the "when" - scheduling the work.
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
            // Read the sync interval from appsettings.json, defaulting to 1 minute if not specified.
            var intervalMinutes = configuration.GetValue<int>("AttendanceSync:SyncIntervalMinutes", 1);
            _syncInterval = TimeSpan.FromMinutes(intervalMinutes);
        }

        /// <summary>
        /// The main execution loop for the background service.
        /// This pattern is more robust than a Timer because it guarantees one sync cycle completes before the next one begins.
        /// </summary>
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("Attendance Sync Service starting.");
            _logger.LogInformation("Sync interval set to {Minutes} minutes.", _syncInterval.TotalMinutes);

            // Wait a few seconds on startup before the first run to ensure all services are initialized.
            await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);

            // Loop indefinitely until the service is requested to stop.
            while (!stoppingToken.IsCancellationRequested)
            {
                await _dataSyncer.SyncDataAsync();
                _logger.LogInformation("Next sync scheduled in {SyncInterval}", _syncInterval);
                await Task.Delay(_syncInterval, stoppingToken);
            }
        }

        public override async Task StopAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Attendance Sync Service stopping...");
            await base.StopAsync(cancellationToken);
            _logger.LogInformation("Attendance Sync Service stopped.");
        }
    }

    /// <summary>
    /// Contains the core business logic for reading data from the Access database
    /// and writing it to the PostgreSQL database. This class is responsible for the "what" and "how".
    /// </summary>
    public class AttendanceDataSyncer
    {
        private readonly ILogger<AttendanceDataSyncer> _logger;
        private readonly string _accessPath;
        private readonly string _neonConnectionString;
        private readonly string _lastSyncFile;

        public AttendanceDataSyncer(ILogger<AttendanceDataSyncer> logger, IConfiguration configuration)
        {
            _logger = logger;
            _accessPath = configuration["AttendanceSync:AccessDatabasePath"]
                ?? throw new InvalidOperationException("AccessDatabasePath is not configured");
            _neonConnectionString = configuration["AttendanceSync:NeonConnectionString"]
                ?? throw new InvalidOperationException("NeonConnectionString is not configured");
            _lastSyncFile = configuration["AttendanceSync:LastSyncFilePath"]
                ?? Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.CommonApplicationData), "AttendanceSyncService", "lastSync.txt");

            // Ensure the directory for the last sync file exists.
            var lastSyncDir = Path.GetDirectoryName(_lastSyncFile);
            if (!string.IsNullOrEmpty(lastSyncDir))
            {
                Directory.CreateDirectory(lastSyncDir);
            }
        }

        /// <summary>
        /// Executes a single data synchronization cycle.
        /// </summary>
        public async Task SyncDataAsync()
        {
            try
            {
                _logger.LogInformation("=== Sync cycle started at {Time} (Local) ===", DateTime.Now);

                // Get the raw start and end times for the sync window.
                DateTime rawLastSyncDate = GetLastSyncDate();
                DateTime rawCurrentSyncDate = DateTime.Now;

                // Truncate the times to the minute to align with the precision of the source database.
                DateTime lastSyncDate = new DateTime(rawLastSyncDate.Year, rawLastSyncDate.Month, rawLastSyncDate.Day, rawLastSyncDate.Hour, rawLastSyncDate.Minute, 0);
                DateTime currentSyncDate = new DateTime(rawCurrentSyncDate.Year, rawCurrentSyncDate.Month, rawCurrentSyncDate.Day, rawCurrentSyncDate.Hour, rawCurrentSyncDate.Minute, 0);

                // If no full minute has passed since the last cycle, skip this run to avoid empty queries.
                if (lastSyncDate >= currentSyncDate)
                {
                    _logger.LogInformation("No new full minute has passed; sync will run in the next cycle.");
                    SaveLastSyncDate(rawCurrentSyncDate);
                    _logger.LogInformation("=== Sync cycle completed successfully ===");
                    return;
                }

                _logger.LogInformation("Syncing records with DownloadDate between {LastSyncDate} and {CurrentSyncDate} (Local)", lastSyncDate, currentSyncDate);
                var attendanceLogs = await GetAttendanceLogsFromAccessAsync(lastSyncDate, currentSyncDate);

                if (!attendanceLogs.Any())
                {
                    _logger.LogInformation("No new records to sync.");
                }
                else
                {
                    _logger.LogInformation("Retrieved {RecordCount} records from local database", attendanceLogs.Count);

                    // Log the retrieved data at Debug level for detailed verification.
                    _logger.LogDebug("--- Data to be synced ---");
                    foreach (var log in attendanceLogs)
                    {
                        _logger.LogDebug(
                            "  - EmployeeID: {EmployeeID}, EventTime: {EventTime}, DownloadDate: {DownloadDate}",
                            log.EmployeeID,
                            log.EventTime,
                            log.DownloadDate);
                    }
                    _logger.LogDebug("--- End of data ---");

                    await InsertIntoNeonDatabaseAsync(attendanceLogs);
                    _logger.LogInformation("Successfully synced {RecordCount} records to remote database", attendanceLogs.Count);
                }

                // Save the exact current time to the file. This becomes the starting point for the next cycle.
                SaveLastSyncDate(rawCurrentSyncDate);
                _logger.LogInformation("=== Sync cycle completed successfully ===");
            }
            catch (Exception ex)
            {
                // If any error occurs, log it and do NOT update the last sync date, ensuring the same period is retried.
                _logger.LogError(ex, "An error occurred during the data sync cycle. The last sync date will not be updated.");
            }
        }

        /// <summary>
        /// Connects to the MS Access database and retrieves attendance logs within the specified date range.
        /// </summary>
        private async Task<List<AttendanceLog>> GetAttendanceLogsFromAccessAsync(DateTime lastSyncDate, DateTime currentSyncDate)
        {
            var logs = new List<AttendanceLog>();
            string connectionString = $"Provider=Microsoft.ACE.OLEDB.12.0;Data Source={_accessPath};Persist Security Info=False;";

            await using var connection = new OleDbConnection(connectionString);
            await connection.OpenAsync();

            var tablesToQuery = GetTableNamesForDateRange(lastSyncDate, currentSyncDate);

            // Format dates into the specific string format Access SQL requires: #M/d/yyyy HH:mm:ss#.
            string formattedLastSync = lastSyncDate.ToString("MM/dd/yyyy HH:mm:ss", CultureInfo.InvariantCulture);
            string formattedCurrentSync = currentSyncDate.ToString("MM/dd/yyyy HH:mm:ss", CultureInfo.InvariantCulture);

            foreach (var tableName in tablesToQuery)
            {
                try
                {
                    // The query now directly embeds the formatted date strings, which is the most reliable way for the OLEDB driver.
                    string query = $@"
                        SELECT [UserId] AS eID, [LogDate] AS evtTime, [DownloadDate] AS dlDate
                        FROM [{tableName}]
                        WHERE [DownloadDate] >= #{formattedLastSync}# AND [DownloadDate] < #{formattedCurrentSync}#";

                    await using var command = new OleDbCommand(query, connection);

                    await using var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        var employeeId = reader["eID"].ToString();
                        if (!string.IsNullOrEmpty(employeeId))
                        {
                            var eventTime = (DateTime)reader["evtTime"];
                            var downloadDate = (DateTime)reader["dlDate"];

                            logs.Add(new AttendanceLog
                            {
                                EmployeeID = employeeId,
                                // Specify that the times read from Access are in the machine's local timezone.
                                EventTime = DateTime.SpecifyKind(eventTime, DateTimeKind.Local),
                                DownloadDate = DateTime.SpecifyKind(downloadDate, DateTimeKind.Local)
                            });
                        }
                    }
                }
                catch (OleDbException ex) when (ex.Message.Contains("cannot find", StringComparison.OrdinalIgnoreCase))
                {
                    // This is an expected case (e.g., a table for a future month doesn't exist yet), so we just log it and continue.
                    _logger.LogDebug("Table {TableName} not found, skipping.", tableName);
                }
            }
            return logs.OrderBy(l => l.DownloadDate).ToList();
        }

        /// <summary>
        /// Generates the list of table names (e.g., "DeviceLogs_10_2025") that fall within the sync date range.
        /// </summary>
        private List<string> GetTableNamesForDateRange(DateTime startDate, DateTime endDate)
        {
            var tableNames = new HashSet<string>();
            var currentDate = startDate;
            while (currentDate <= endDate)
            {
                tableNames.Add($"DeviceLogs_{currentDate.Month}_{currentDate.Year}");
                currentDate = currentDate.AddMonths(1);
            }
            return tableNames.ToList();
        }

        /// <summary>
        /// Inserts a list of attendance logs into the PostgreSQL database using the highly efficient COPY command.
        /// </summary>
        private async Task InsertIntoNeonDatabaseAsync(List<AttendanceLog> logs)
        {
            await using var connection = new NpgsqlConnection(_neonConnectionString);
            await connection.OpenAsync();

            try
            {
                // The COPY command is atomic. It will either succeed or fail as a whole, so a manual transaction is not needed.
                await using var writer = await connection.BeginBinaryImportAsync(
                    "COPY attendance_logs (employee_id, event_time, download_date) FROM STDIN (FORMAT BINARY)");

                foreach (var log in logs)
                {
                    await writer.StartRowAsync();
                    await writer.WriteAsync(log.EmployeeID, NpgsqlDbType.Varchar);
                    // Send the local time directly to the "timestamp without time zone" column.
                    await writer.WriteAsync(log.EventTime, NpgsqlDbType.Timestamp);
                    await writer.WriteAsync(log.DownloadDate, NpgsqlDbType.Timestamp);
                }

                await writer.CompleteAsync();
            }
            catch (Exception)
            {
                // The exception is re-thrown to be caught and logged by SyncDataAsync.
                throw;
            }
        }

        /// <summary>
        /// Retrieves the timestamp of the last successful synchronization from a file.
        /// </summary>
        private DateTime GetLastSyncDate()
        {
            try
            {
                if (File.Exists(_lastSyncFile))
                {
                    string content = File.ReadAllText(_lastSyncFile).Trim();
                    if (DateTime.TryParse(content, null, DateTimeStyles.RoundtripKind, out DateTime lastSync))
                    {
                        return lastSync;
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error reading last sync date. Defaulting to earliest time.");
            }

            // Fallback for the first run: return the earliest possible time to sync all historical data.
            return DateTime.MinValue;
        }

        /// <summary>
        /// Saves the timestamp of the current synchronization to a file for state persistence.
        /// </summary>
        private void SaveLastSyncDate(DateTime syncDate)
        {
            try
            {
                // Use the "o" format specifier for a round-trippable, culture-invariant string.
                File.WriteAllText(_lastSyncFile, syncDate.ToString("o", CultureInfo.InvariantCulture));
                _logger.LogInformation("Last sync date saved as: {SyncDate} (Local)", syncDate);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "FATAL: Could not save the last sync date. This may cause duplicate data processing on the next run.");
            }
        }
    }

    /// <summary>
    /// Represents a single attendance log record.
    /// </summary>
    public class AttendanceLog
    {
        public required string EmployeeID { get; set; }
        public DateTime EventTime { get; set; }
        public DateTime DownloadDate { get; set; }
    }
}