// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { createContext, useCallback, useContext, useMemo } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import ApiGateway from '../gateways/Api.gateway';
import { CartItem, OrderResult, PlaceOrderRequest } from '../protos/demo';
import { IProductCart } from '../types/Cart';
import { useCurrency } from './Currency.provider';
import { HttpError } from '../utils/Request';

const retryTransient = (failureCount: number, error: Error) =>
  failureCount < 2 && (!(error instanceof HttpError) || error.status === 408 || error.status === 429 || error.status >= 500);

interface IContext {
  cart: IProductCart;
  addItem(item: CartItem): void;
  updateItemQuantity(productId: string, newQuantity: number): void;
  emptyCart(): void;
  placeOrder(order: Omit<PlaceOrderRequest, 'operationId'>): Promise<OrderResult>;
}

export const Context = createContext<IContext>({
  cart: { userId: '', items: [] },
  addItem: () => {},
  updateItemQuantity: () => {},
  emptyCart: () => {},
  placeOrder: () => Promise.resolve({} as OrderResult),
});

interface IProps {
  children: React.ReactNode;
}

export const useCart = () => useContext(Context);

const CartProvider = ({ children }: IProps) => {
  const { selectedCurrency } = useCurrency();
  const queryClient = useQueryClient();
  const mutationOptions = useMemo(
    () => ({
      onSuccess: () => {
        queryClient.invalidateQueries({ queryKey: ['cart'] });
      },
    }),
    [queryClient]
  );

  const { data: cart = { userId: '', items: [] } } = useQuery({
    queryKey: ['cart', selectedCurrency],
    queryFn: () => ApiGateway.getCart(selectedCurrency),
    retry: retryTransient,
  });
  const addCartMutation = useMutation({
    mutationFn: ApiGateway.addCartItem,
    retry: retryTransient,
    ...mutationOptions,
  });

  const emptyCartMutation = useMutation({
    mutationFn: ApiGateway.emptyCart,
    retry: retryTransient,
    ...mutationOptions,
  });

  const placeOrderMutation = useMutation({
    mutationFn: ApiGateway.placeOrder,
    ...mutationOptions,
  });

  const addItem = useCallback(
    (item: CartItem) => addCartMutation.mutateAsync({ ...item, currencyCode: selectedCurrency, operationId: crypto.randomUUID() }),
    [addCartMutation, selectedCurrency]
  );

  const updateItemQuantity = useCallback(
    (productId: string, newQuantity: number) => {
      const existing = cart.items.find(i => i.productId === productId);
      const delta = newQuantity - (existing?.quantity ?? 0);
      if (delta !== 0) {
        addCartMutation.mutateAsync({ productId, quantity: delta, currencyCode: selectedCurrency, operationId: crypto.randomUUID() });
      }
    },
    [addCartMutation, cart.items, selectedCurrency]
  );
  const emptyCart = useCallback(() => emptyCartMutation.mutateAsync({ operationId: crypto.randomUUID() }), [emptyCartMutation]);
  const placeOrder = useCallback(
    (order: Omit<PlaceOrderRequest, 'operationId'>) =>
      placeOrderMutation.mutateAsync({ ...order, operationId: crypto.randomUUID(), currencyCode: selectedCurrency }),
    [placeOrderMutation, selectedCurrency]
  );

  const value = useMemo(() => ({ cart, addItem, updateItemQuantity, emptyCart, placeOrder }), [cart, addItem, updateItemQuantity, emptyCart, placeOrder]);

  return <Context.Provider value={value}>{children}</Context.Provider>;
};

export default CartProvider;
