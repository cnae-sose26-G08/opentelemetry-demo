// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0
using System.Threading.Tasks;
using System.Threading;

namespace cart.cartstore;

public interface ICartStore
{
    void Initialize();

    Task AddItemAsync(string userId, string productId, int quantity, string operationId, CancellationToken cancellationToken);
    Task EmptyCartAsync(string userId, string operationId, CancellationToken cancellationToken);

    Task<Oteldemo.Cart> GetCartAsync(string userId, CancellationToken cancellationToken);

    bool Ping();
}
