// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;
using Oteldemo;
using Microsoft.EntityFrameworkCore;
using System.Diagnostics;

namespace Accounting;

internal class DBContext : DbContext
{
    private readonly string _connectionString;

    public DBContext(string connectionString)
    {
        _connectionString = connectionString;
    }

    public DbSet<OrderEntity> Orders { get; set; }
    public DbSet<OrderItemEntity> CartItems { get; set; }
    public DbSet<ShippingEntity> Shipping { get; set; }

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        optionsBuilder.UseNpgsql(_connectionString).UseSnakeCaseNamingConvention();
    }
}


internal class Consumer : BackgroundService
{
    private const string TopicName = "orders";
    private const int MaxMessageRetryAttempts = 7;

    private readonly ILogger _logger;
    private readonly IConsumer<string, byte[]> _consumer;
    private readonly string? _dbConnectionString;
    private static readonly ActivitySource MyActivitySource = new("Accounting.Consumer");

    private enum ProcessingResult
    {
        Persisted,
        Duplicate,
        RetryableFailure,
        FatalInvalidMessage,
    }

    public Consumer(ILogger<Consumer> logger)
    {
        _logger = logger;

        var servers = Environment.GetEnvironmentVariable("KAFKA_ADDR")
            ?? throw new InvalidOperationException("The KAFKA_ADDR environment variable is not set.");

        _consumer = BuildConsumer(servers);
        _consumer.Subscribe(TopicName);

        Log.KafkaConnecting(_logger, servers);

        _dbConnectionString = Environment.GetEnvironmentVariable("DB_CONNECTION_STRING");
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Yield();

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    using var activity = MyActivitySource.StartActivity("order-consumed",  ActivityKind.Internal);
                    var consumeResult = _consumer.Consume(stoppingToken);
                    var result = ProcessingResult.RetryableFailure;
                    for (var attempt = 0; attempt < MaxMessageRetryAttempts && result == ProcessingResult.RetryableFailure; attempt++)
                    {
                        result = await ProcessMessageAsync(consumeResult.Message, stoppingToken);
                        if (result == ProcessingResult.RetryableFailure && attempt + 1 < MaxMessageRetryAttempts)
                        {
                            await Task.Delay(TimeSpan.FromSeconds(Math.Min(1 << attempt, 5)), stoppingToken);
                        }
                    }
                    if (result is ProcessingResult.Persisted or ProcessingResult.Duplicate or ProcessingResult.FatalInvalidMessage)
                    {
                        _consumer.Commit(consumeResult);
                    }
                    else
                    {
                        _logger.LogError("Order persistence retry budget exhausted; seeking to the failed Kafka record without committing its offset.");
                        _consumer.Seek(consumeResult.TopicPartitionOffset);
                        await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
                    }
                }
                catch (ConsumeException e)
                {
                    Log.ConsumeError(_logger, e, e.Error.Reason);
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
        finally
        {
            Log.ConsumerClosing(_logger);
            _consumer.Close();
        }
    }

    private async Task<ProcessingResult> ProcessMessageAsync(Message<string, byte[]> message, CancellationToken stoppingToken)
    {
        OrderResult order;
        try
        {
            order = OrderResult.Parser.ParseFrom(message.Value);
            if (string.IsNullOrWhiteSpace(order.OrderId))
            {
                Log.OrderParsingFailed(_logger, new InvalidOperationException("order_id is required"));
                return ProcessingResult.FatalInvalidMessage;
            }
            Log.OrderReceivedMessage(_logger, order);
        }
        catch (Exception ex)
        {
            Log.OrderParsingFailed(_logger, ex);
            return ProcessingResult.FatalInvalidMessage;
        }

        if (_dbConnectionString == null)
        {
            Log.OrderParsingFailed(_logger, new InvalidOperationException("DB_CONNECTION_STRING is not set"));
            return ProcessingResult.RetryableFailure;
        }

        try
        {
            using var dbContext = new DBContext(_dbConnectionString);
            await using var transaction = await dbContext.Database.BeginTransactionAsync(stoppingToken);
            dbContext.Add(new OrderEntity { Id = order.OrderId });
            foreach (var item in order.Items)
            {
                dbContext.Add(new OrderItemEntity
                {
                    ItemCostCurrencyCode = item.Cost.CurrencyCode,
                    ItemCostUnits = item.Cost.Units,
                    ItemCostNanos = item.Cost.Nanos,
                    ProductId = item.Item.ProductId,
                    Quantity = item.Item.Quantity,
                    OrderId = order.OrderId
                });
            }

            dbContext.Add(new ShippingEntity
            {
                ShippingTrackingId = order.ShippingTrackingId,
                ShippingCostCurrencyCode = order.ShippingCost.CurrencyCode,
                ShippingCostUnits = order.ShippingCost.Units,
                ShippingCostNanos = order.ShippingCost.Nanos,
                StreetAddress = order.ShippingAddress.StreetAddress,
                City = order.ShippingAddress.City,
                State = order.ShippingAddress.State,
                Country = order.ShippingAddress.Country,
                ZipCode = order.ShippingAddress.ZipCode,
                OrderId = order.OrderId
            });
            await dbContext.SaveChangesAsync(stoppingToken);
            await transaction.CommitAsync(stoppingToken);
            return ProcessingResult.Persisted;
        }
        catch (DbUpdateException ex) when (ex.InnerException is PostgresException { SqlState: PostgresErrorCodes.UniqueViolation })
        {
            Log.DuplicateOrderSkipped(_logger);
            return ProcessingResult.Duplicate;
        }
        catch (Exception ex)
        {
            Log.OrderParsingFailed(_logger, ex);
            return ProcessingResult.RetryableFailure;
        }
    }

    private static IConsumer<string, byte[]> BuildConsumer(string servers)
    {
        var conf = new ConsumerConfig
        {
            GroupId = "accounting",
            BootstrapServers = servers,
            // https://github.com/confluentinc/confluent-kafka-dotnet/tree/07de95ed647af80a0db39ce6a8891a630423b952#basic-consumer-example
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false,
        };

        return new ConsumerBuilder<string, byte[]>(conf)
            .Build();
    }

    public override void Dispose()
    {
        _consumer?.Dispose();
        base.Dispose();
    }
}
