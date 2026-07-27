// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0
using System.Threading.Tasks;

namespace cart.cartstore;

public interface ICartStore
{
    void Initialize();

    Task AddItemAsync(string userId, string productId, int quantity, string operationId);
    Task EmptyCartAsync(string userId, string operationId);

    Task<Oteldemo.Cart> GetCartAsync(string userId);

    bool Ping();
}
