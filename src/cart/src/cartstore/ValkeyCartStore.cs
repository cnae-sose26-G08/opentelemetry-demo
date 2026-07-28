// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0
using System;
using System.Threading.Tasks;
using Grpc.Core;
using StackExchange.Redis;
using Google.Protobuf;
using Microsoft.Extensions.Logging;
using System.Diagnostics.Metrics;
using System.Diagnostics;
using System.Threading;
using System.Security.Cryptography;
using System.Text;

namespace cart.cartstore;

public class ValkeyCartStore : ICartStore
{
    private sealed class TransientCartStoreException(string message) : Exception(message)
    {
    }

    private readonly ILogger _logger;
    private const int RedisRetryNumber = 30;
    private static readonly TimeSpan CartTtl = TimeSpan.FromHours(2);
    private static readonly TimeSpan OperationTtl = TimeSpan.FromHours(2);
    private static readonly TimeSpan OperationDeadline = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan[] RetryDelays =
    [
        TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(250), TimeSpan.FromMilliseconds(500),
        TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(4), TimeSpan.FromSeconds(5),
    ];
    private const string AddItemScript = """
        local existing = redis.call('HGET', KEYS[2], ARGV[1])
        if existing then
          if existing ~= ARGV[2] then return 'CONFLICT' end
          return tostring(redis.call('HGET', KEYS[1], ARGV[3]) or '0')
        end
        redis.call('HSET', KEYS[2], ARGV[1], ARGV[2])
        local quantity = redis.call('HINCRBY', KEYS[1], ARGV[3], ARGV[4])
        if quantity <= 0 then
          redis.call('HDEL', KEYS[1], ARGV[3])
          quantity = 0
        end
        redis.call('EXPIRE', KEYS[1], ARGV[5])
        redis.call('EXPIRE', KEYS[2], ARGV[6])
        return tostring(quantity)
        """;
    private const string EmptyCartScript = """
        local existing = redis.call('HGET', KEYS[2], ARGV[1])
        if existing then
          if existing ~= ARGV[2] then return 'CONFLICT' end
          return 'OK'
        end
        redis.call('HSET', KEYS[2], ARGV[1], ARGV[2])
        redis.call('DEL', KEYS[1])
        redis.call('EXPIRE', KEYS[2], ARGV[3])
        return 'OK'
        """;

    private volatile ConnectionMultiplexer _redis;
    private volatile bool _isRedisConnectionOpened;

    private readonly object _locker = new();
    private readonly string _connectionString;

    private static readonly ActivitySource CartActivitySource = new("OpenTelemetry.Demo.Cart");
    private static readonly Meter CartMeter = new Meter("OpenTelemetry.Demo.Cart");
    private static readonly Histogram<double> addItemHistogram = CartMeter.CreateHistogram(
        "demo.cart.add_item.latency",
        unit: "s",
        advice: new InstrumentAdvice<double>
        {
            HistogramBucketBoundaries = [0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10]
        });
    private static readonly Histogram<double> getCartHistogram = CartMeter.CreateHistogram(
        "demo.cart.get_cart.latency",
        unit: "s",
        advice: new InstrumentAdvice<double>
        {
            HistogramBucketBoundaries = [0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10]
        });
    private readonly ConfigurationOptions _redisConnectionOptions;

    public ValkeyCartStore(ILogger<ValkeyCartStore> logger, string valkeyAddress)
    {
        _logger = logger;
        _connectionString = $"{valkeyAddress},ssl=false,allowAdmin=true,abortConnect=false";

        _redisConnectionOptions = ConfigurationOptions.Parse(_connectionString);

        // Try to reconnect multiple times if the first retry fails.
        _redisConnectionOptions.ConnectRetry = RedisRetryNumber;
        _redisConnectionOptions.ReconnectRetryPolicy = new ExponentialRetry(1000);

        _redisConnectionOptions.KeepAlive = 180;
    }

    public ConnectionMultiplexer GetConnection()
    {
        EnsureRedisConnected();
        return _redis;
    }

    public void Initialize()
    {
        EnsureRedisConnected();
    }

    private void EnsureRedisConnected()
    {
        if (_isRedisConnectionOpened)
        {
            return;
        }

        // Connection is closed or failed - open a new one but only at the first thread
        lock (_locker)
        {
            if (_isRedisConnectionOpened)
            {
                return;
            }

            Log.RedisConnecting(_logger, _connectionString);

            var redis = ConnectionMultiplexer.Connect(_redisConnectionOptions);

            if (!redis.IsConnected)
            {
                Log.RedisConnectionFailed(_logger);
                redis.Dispose();
                throw new TransientCartStoreException("Wasn't able to connect to Valkey");
            }

            _redis = redis;

            Log.RedisConnected(_logger);
            var cache = _redis.GetDatabase();

            Log.RedisSmallTest(_logger);
            cache.StringSet("cart", "OK");
            string res = (string)cache.StringGet("cart");

            Log.RedisSmallTestResult(_logger, res);

            _redis.InternalError += (_, e) => { Log.RedisInternalError(_logger, e.Exception); };
            _redis.ConnectionRestored += (_, _) =>
            {
                _isRedisConnectionOpened = true;
                Log.RedisConnectionRestored(_logger);
            };
            _redis.ConnectionFailed += (_, _) =>
            {
                Log.RedisConnectionLost(_logger);
                _isRedisConnectionOpened = false;
            };

            _isRedisConnectionOpened = true;
        }
    }

    public async Task AddItemAsync(string userId, string productId, int quantity, string operationId, CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();

        Log.AddItemAsync(_logger, userId, productId, quantity);

        try
        {
            RequireOperationId(operationId);
            var itemKey = ItemKey(userId);
            var fingerprint = Fingerprint($"v1\0add\0{productId.Length}:{productId}\0{quantity}");
            await RetryTransientAsync(async () =>
            {
                EnsureRedisConnected();
                var result = await _redis.GetDatabase().ScriptEvaluateAsync(AddItemScript,
                    [itemKey, OperationKey(userId)],
                    [operationId, fingerprint, productId, quantity, (long)CartTtl.TotalSeconds, (long)OperationTtl.TotalSeconds]);
                ThrowIfConflict(result);
                return true;
            }, cancellationToken);
        }
        catch (RpcException)
        {
            throw;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new RpcException(new Status(StatusCode.FailedPrecondition, $"Can't access cart storage. {ex}"));
        }
        finally
        {
            addItemHistogram.Record(stopwatch.Elapsed.TotalSeconds);
        }
    }

    public async Task EmptyCartAsync(string userId, string operationId, CancellationToken cancellationToken)
    {
        Log.EmptyCartAsync(_logger, userId);
        try
        {
            RequireOperationId(operationId);
            await RetryTransientAsync(async () =>
            {
                EnsureRedisConnected();
                var result = await _redis.GetDatabase().ScriptEvaluateAsync(EmptyCartScript,
                    [ItemKey(userId), OperationKey(userId)],
                    [operationId, Fingerprint("v1\0empty"), (long)OperationTtl.TotalSeconds]);
                ThrowIfConflict(result);
                return true;
            }, cancellationToken);
        }
        catch (RpcException)
        {
            throw;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new RpcException(new Status(StatusCode.FailedPrecondition, $"Can't access cart storage. {ex}"));
        }
    }

    public async Task<Oteldemo.Cart> GetCartAsync(string userId, CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();

        Log.GetCartAsync(_logger, userId);

        try
        {
            var values = await RetryTransientAsync(async () =>
            {
                EnsureRedisConnected();
                return await _redis.GetDatabase().HashGetAllAsync(ItemKey(userId));
            }, cancellationToken);
            var cart = new Oteldemo.Cart { UserId = userId };
            foreach (var value in values)
            {
                cart.Items.Add(new Oteldemo.CartItem { ProductId = value.Name.ToString(), Quantity = (int)value.Value });
            }
            return cart;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new RpcException(new Status(StatusCode.FailedPrecondition, $"Can't access cart storage. {ex}"));
        }
        finally
        {
            getCartHistogram.Record(stopwatch.Elapsed.TotalSeconds);
        }
    }

    public bool Ping()
    {
        try
        {
            var cache = _redis.GetDatabase();
            var res = cache.Ping();
            return res != TimeSpan.Zero;
        }
        catch (Exception)
        {
            return false;
        }
    }

    private static string ItemKey(string userId) => $"cart:{{{userId}}}:items";

    private static string OperationKey(string userId) => $"cart:{{{userId}}}:operations";

    private static void RequireOperationId(string operationId)
    {
        if (string.IsNullOrWhiteSpace(operationId))
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, "operation_id is required"));
        }
    }

    private static bool IsTransient(Exception exception) =>
        exception is RedisConnectionException or RedisTimeoutException or TransientCartStoreException;

    private static string Fingerprint(string payload) => Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(payload)));

    private static void ThrowIfConflict(RedisResult result)
    {
        if (result.ToString() == "CONFLICT")
        {
            throw new RpcException(new Status(StatusCode.AlreadyExists, "operation_id was already used with a different payload"));
        }
    }

    private static async Task<T> RetryTransientAsync<T>(Func<Task<T>> operation, CancellationToken cancellationToken)
    {
        var deadline = DateTime.UtcNow + OperationDeadline;
        var attempt = 0;
        while (true)
        {
            try
            {
                cancellationToken.ThrowIfCancellationRequested();
                return await operation();
            }
            catch (Exception ex) when (IsTransient(ex) && DateTime.UtcNow < deadline)
            {
                var delay = RetryDelays[Math.Min(attempt++, RetryDelays.Length - 1)];
                if (DateTime.UtcNow + delay > deadline) delay = deadline - DateTime.UtcNow;
                if (delay > TimeSpan.Zero) await Task.Delay(delay, cancellationToken);
            }
        }
    }
}
