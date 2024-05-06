#include <numeric> // Include the necessary header file for the 'std::accumulate' function.
#include <iostream>
#include <cstdint>
#include <vector>
#include <list>
#include <memory>
#include <format>
#include <chrono>
#include <time>

void Orderbook::PruneGoodForDayOrders()
{
     using namespace std::chrono;
     const auto end = hours(16);
        while (true)
        {
                const auto now = system_clock::now();
                const auto now_c = system_clock::to_time_t(now);
                std::tm now_parts;
                localtime_s(&now_parts, &now_c);

                if (now_parts.tm_hour >= end.count())
                    now_parts.tm_mday += 1;

                now_parts.tm_hour = end.count();
                now_parts.tm_min = 0;
                now_parts.tm_sec = 0;

                auto next = system_clock::from_time_t(mktime(&now_parts));
                auto till = next - now + milliseconds(100);

                {
                    std::unique_lock ordersLock{ ordersMutex_ };
                    if (shutdown_.load(std::memory_order_acquire) ||
                            shutdownConditionVariable_.wait_for(ordersLock, till) == std::cv_status::no_timeout)
                            return;
                }
                OrderIds orderIds;
                {
                    std::scoped_lock ordersLock{ ordersMutex_ };
                    for (const auto& [_, entry] : orders_)
                    {
                        const auto& [order, _] = entry;

                        if (order->GetOrderType() != OrderType::GoodForDay)\
                                continue;
                        orderIds.push_back(order->GetOrderId());

                    }
                }
            CancelOrders(orderIds);
        }
}

enum class OrderType
{
    GoodTillCancel,
    FillAndKill,
    FillOrKill,
    GoodForDay,
    Market,
};

enum class Side
{
    Buy,
    Sell
};

#pragma once

#include <limits>

#include "Usings.h"

struct Constants
{
    static const Price InvalidPrice = std::numeric_limits<Price>::quiet_NaN();
};

using Price = std::int32_t;
using Quantity = std::uint32_t;
using OrderId = std::uint32_t;

struct LevelInfo {
    Price price_;
    Quantity quantity_;
};

using LevelInfos = std::vector<LevelInfo>;

class OrderbookLevelInfos 
{
public:
    OrderbookLevelInfos(const LevelInfos& bids, const LevelInfos& asks)
        : bids_{ bids }
        , asks_{ asks }
    { }
    const LevelInfos& GetBids() const { return bids_; }
    const LevelInfos& GetAsks() const { return asks_; }
private:
    LevelInfos bids_;
    LevelInfos asks_;
};

class Order 
{
public:
    Order(OrderType orderType, OrderId orderId, Side side, Price price, Quantity quantity)
        : orderType_{ orderType }
        , orderId_{ orderId }
        , side_{ side }
        , price_{ price }
        , initialQuantity_{ quantity }
        , remainingQuantity_{ quantity }
        { }

        Order(OrderId orderId, Side side, Quantity quantity)
            : Order(OrderType::Market, orderId, side, Constants::InvalidPrice, quantity)
        { }
    OrderId GetOrderId() const { return orderId_; }
    Side GetSide() const { return side_; }
    Price GetPrice() const { return price_; }
    OrderType GetOrderType() const { return orderType_; }
    Quantity GetInitialQuantity() const { return initialQuantity_; }
    Quantity GetRemainingQuantity() const { return remainingQuantity_; }
    Quantity GetFilledQuantity() const {return GetInitialQuantity() - GetRemainingQuantity(); }
    bool isFilled() const { return GetRemainingQuantity() == 0;}

    void Fill (Quantity quantity) {
        if (quantity > GetRemainingQuantity())
            throw std::logic_error(std::format("Order ({}) cannot be filled for more than its remaining quantity.", GetOrderId()));
        remainingQuantity_ -= quantity;
    }
private:
    OrderType orderType_;
    OrderId orderId_;
    Side side_;
    Price price_;
    Quantity initialQuantity_;
    Quantity remainingQuantity_;
};

using OrderPointer = std::shared_ptr<Order>;
using OrderPointers = std::list<OrderPointer>;

class OrderModify
{
public:
    OrderModify(OrderId orderId, Side side, Price price, Quantity quantity)
        : orderId_{ orderId }
        , side_{ side }
        , price_{ price }
        , quantity_{ quantity }
    { }

    OrderId GetOrderId() const { return orderId_; }
    Price GetPrice() const { return price_; }
    Side GetSide() const { return side_; }
    Quantity GetQuantity() const { return quantity_; }

    OrderPointer ToOrderPoint(OrderType type) const
    {
        return std::make_shared<Order>(type, GetOrderId(), GetPrice(), GetQuantity());
    }
private:
    OrderId orderId_;
    Price price_;
    Side side_;
    Quantity quantity_;
};

struct TradeInfo
{
    OrderId orderId_;
    Price price_;
    Quantity quantity_;
};

class Trade
{
public:
    Trade(const TradeInfo& bidTrade, const TradeInfo& askTrade)
        : bidTrade_{ bidTrade }
        , askTrade_{ askTrade }
    { }

    const TradeInfo& GetBidTrade() const { return bidTrade_; }
    const TradeInfo& GetAskTrade() const { return askTrade_; }
private:
    TradeInfo bidTrade_;
    TradeInfo askTrade_;
};

using Trades = std::vector<Trade>;

#include <map> // Include the necessary header file for the 'std::map' container.
#include <functional> // Include the necessary header file for the 'std::greater' and 'std::less' comparators.

class OrderBook
{
private:
    struct OrderEntry
    {
        OrderPointer order_{ nullptr };
        OrderPointers::iterator location_;
    };
    struct LevelData
    {
        Quantity quantity_{ };
        Quantity count_{ };

        enum class Action
        {
            Add,
            Remove,
            Match,
        };
    };
    std::unordered_map<Price, LevelData> data_;
    std::map<Price, OrderPointers, std::greater<Price>> bids_;
    std::map<Price, OrderPointers, std::less<Price>> asks_;
    std::unordered_map<OrderId, OrderEntry> orders_;
    mutable std::mutex ordersMutex_;
    std::thread ordersPruneThread_;
    std::condition_variable shutdownConditionVariable_;
    std::atomic<bool> shutdown_{ false };

    void PruneGoodForDayOrders();

    void CancelOrders(OrderIds orderIds);
    void CancelOrderInternal(OrderId orderId);

    void OnOrderCancelled(OrderPointer order);
    void OnOrderAdded(OrderPointer order);
    void OnOrderMatched(Price price, Quantity quantity, bool isFullyFilled);
    void UpdateLevelData(Price price, Quantity quantity, LevelData::Action action);

    bool CanFullyFill(Side side, Price price, Quantity quantity) const;
    bool CanMatch(Side side, Price price) const
    {
        if (side == Side::Buy)
        {
            if (asks_.empty())
                return false; // cant match
            const auto& [bestAsk, _] = *asks_.begin();
            return price >= bestAsk;
        }
        else
        {
            if (bids_.empty())
                return false; // cant match
            const auto& [bestBid, _] = *bids_.begin();
            return price <= bestBid;
        }
    }

    Trades MatchOrders()
    {
        Trades trades;
        trades.reserve(orders_.size());

        while(true)
        {
            if (bids_.empty() || asks_.empty())
                break;
            
            auto& [bidPrice, bids] = *bids_.begin();
            auto& [askPrice, asks] = *asks_.begin();

            if (bidPrice < askPrice)
                break;
            
            while (bids.size() && asks.size())
            {
                auto& bid = bids.front();
                auto& ask = asks.front();

                Quantity quantity = std::min(bid->GetRemainingQuantity(), ask->GetRemainingQuantity());
                bid->Fill(quantity);
                ask->Fill(quantity);

                if (bid->isFilled())
                {
                    bids.pop_front();
                    orders_.erase(bid->GetOrderId());
                }

                if (bids.empty())
                    bids_.erase(bidPrice);
                if (asks.empty())
                    asks_.erase(askPrice);
                
                trades.push_back(Trade{
                    TradeInfo{ bid->GetOrderId(), bid->GetPrice(), quantity},
                    TradeInfo{ ask->GetOrderId(), ask->GetPrice(), quantity}
                });
            }
        }
        if (!bids_.empty())
        {
            auto& [_, bids] = *bids_.begin();
            auto& order = bids.front();
            if (order->GetOrderType() == OrderType::FillAndKill)
                CancelOrder(order->GetOrderId());
        }
        if (!asks_.empty())
        {
            auto& [_, asks] = *asks_.begin();
            auto& order = asks.front();
            if (order->GetOrderType() == OrderType::FillAndKill)
                CancelOrder(order->GetOrderId());
        }
        return trades;
    }
    Orderbook::Orderbook(): orderPruneThread_{ [this] {PruneGoodForDayOrders();}} { }\
    Orderbook::~Orderbook()
{
    shutdown_.store(true, std::memory_order_release);
	shutdownConditionVariable_.notify_one();
	ordersPruneThread_.join();
}
public:
    Trades Orderbook::AddOrder(OrderPointer order)
    {
        std::scoped_lock ordersLock{ ordersMutex_};
        
        if (orders_.contains(order->GetOrderId()))
            return {};
        if (order->GetOrderType() == OrderType::FillAndKill && !CanMatch(order->GetSide(), order->GetPrice()))
            {
                if (order->GetSide() == Side::Buy && !asks_.empty())
                {
                    const auto& [worstAsk, _] = *asks_.rbegin();
                    order->ToGoodTillCancel(worstAsk);
                }
                else if (order->GetSide() == Side::Sell && !bids_.empty())
                {
                    const auto& [worstBid, _] = *bids_.rbegin();
                    order->ToGoodTillCancel(worstBid);
                }
		else
            return {};
            }
            if (order->GetOrderType() == OrderType::FillAndKill && !CanMatch(order->GetSide(), order->GetPrice()))
                return { };
            if (order->GetOrderType() == OrderType::FillOrKill && !CanFullyFill(order->GetSide(), order->GetPrice(), order->GetInitialQuantity()))
                return { };
        OrderPointers::iterator iterator;

        if (order->GetSide() == Side::Buy)
        {
            auto& orders = bids_[order->GetPrice()];
            orders.push_back(order);
            iterator = std::next(orders.begin(), orders.size() - 1);
        }
        else
        {
            auto& orders = asks_[order->GetPrice()];
            orders.push_back(order);
            iterator = std::next(orders.begin(), orders.size() - 1);
        }
        orders_.insert({order->GetOrderId(), OrderEntry{ order, iterator} });
        OnOrderAdded(order);
        return MatchOrders(); // run matching algorithm
    }
    
    void Orderbook::CancelOrderInternal(OrderId orderId)
    {
        std::scoped_lock ordersLock{ ordersMutex_ };

        for (const auto& orderId : orderIds)
                CancelOrderInternal(orderId);
    }
    
    void Orderbook::CancelOrderInternal(OrderId orderId)
    {
        if (!orders_.contains(orderId))
            return;
        const auto [order, iterator] = orders_.at(orderId);
        orders_.erase(orderId);
        if (order->GetSide() == Side::Sell)
        {
            auto price = order->GetPrice();
            auto& orders = asks_.at(price);
            orders.erase(iterator);
            if (orders.empty())
                asks_.erase(price);
        }
        else
        {
            auto price = order->GetPrice();
            auto& orders = bids_.at(price);
            orders.erase(iterator);
            if (orders.empty())
                bids_.erase(price);
        }
        OnOrderCancelled(order);
    }
    void Orderbook::OnOrderCancelled(OrderPointer order)
    {
        UpdateLevelData(order->GetPrice(), order->GetRemainingQuantity(), LevelData::Action::Remove);
    }
    void Orderbook::OnOrderAdded(OrderPointer order)
    {
        UpdateLevelData(order->GetPrice(), order->GetInitialQuantity(), LevelData::Action::Add);
    }
    void Orderbook::OnOrderMatched(Price price, Quantity quantity, bool isFullyFilled)
    {
        UpdateLevelData(price, quantity, isFullyFilled ? LevelData::Action::Remove : LevelData::Action::Match);
    }
    void Orderbook::UpdateLevelData(Price price, Quantity quantity, LevelData::Action action)
    {
            auto& data = data_[price];
            data.count_ += action == LevelData::Action::Remove ? -1 : action == LevelData::Action::Add ? 1 : 0;
            if (action == LevelData::Action::Remove || action == LevelData::Action::Match)
            {
                data.quantity_ -= quantity;
            }
            else
            {
                data.quantity_ += quantity;
            }
            if (data.count_ == 0)
                data_.erase(price);
    }
    bool Orderbook::CanFullyFill(Side side, Price price, Quantity quantity) const
    {
        if (!CanMatch(side, price))
            return false;
        
        std::optional<Price> threshold;

        if (side == Side::Buy)
        {
            const auto [askPrice, _] = *asks_.begin();
            threshold = askPrice;
        }
        else
        {
            const auto [bidPrice, _] = *bids_.begin();
            threshold = bidPrice;
        }

        for (const auto& [levelPrice, levelData] : data_)
        {
            if (threshold.has_value() &&
                (side == Side::Buy && threshold.value() > levelPrice) ||
                (side == Side::Sell && threshold.value() < levelPrice))
                continue;
            if ((side == Side::Buy && levelPrice > price) ||
                (side == Side::Sell && levelPrice < price))
                continue;
            if (quantity <= levelData.quantity_)
                return true;
            quantity -= levelData.quantity_;
        }
        return false;
    }
    bool Orderbook::CanMatch(Side side, Price price) const
    {
        if (side == Side::Buy)
        {
            if (asks_.empty())
                return false;

            const auto& [bestAsk, _] = *asks_.begin();
            return price >= bestAsk;
        }
        else
        {
            if (bids_.empty())
                return false;
            
            const auto& [bestBid, _] = *bids_.begin();
            return price <= bestBid;
        }
    }
    
    
    
    void Orderbook::CancelOrder(OrderId orderId)
    {
        std::scoped_lock ordersLock{ ordersMutex_ };
        CancelOrderInternal(orderId);
    }
    Trades Orderbook::ModifyOrder(OrderModify order)
    {
        OrderType orderType;
        {
            std::scoped_lock ordersLock{ ordersMutex_ };

            if (!orders_.contains(order.GetOrderId()))
                return { };
            const auto& [existingOrder, _] = orders_.at(order.GetOrderId());
            orderType = existingOrder->GetOrderType();
        }
        CancelOrder(order.GetOrderId());
        return AddOrder(order.ToOrderPoint(existingOrder->GetOrderType()));
    }

    std::size_t Orderbook::Size() const
    {
        std::scoped_lock ordersLock{ ordersMutex_ };
        return orders_.size(); 
    }

    OrderbookLevelInfos Orderbook::GetOrderInfos() const
    {
        LevelInfos bidInfos, askInfos;
        bidInfos.reserve(orders_.size());
        bidInfos.reserve(asks_.size());

        auto CreateLevelInfos = [](Price price, const OrderPointers& orders)
        {
            return LevelInfo(price, std::accumulate(orders.begin(), orders.end(), (Quantity)0,
            [](std::size_t runningSum, const OrderPointer& order)
            {
                return runningSum + order->GetRemainingQuantity();
            }));
        };
        for (const auto& [price, orders] : bids_)
            bidInfos.push_back(CreateLevelInfos(price, orders));
        for (const auto& [price, orders] : asks_)
            askInfos.push_back(CreateLevelInfos(price, orders));
        
        return OrderbookLevelInfos(bidInfos, askInfos);
    }
};

int main() {
    OrderBook orderbook;
    const OrderId orderId = 1;
    orderbook.AddOrder(std::make_shared<Order>(OrderType::GoodTillCancel, orderId, Side::Buy, 100, 10));
    std::cout << orderbook.Size() << std::endl;
    orderbook.CancelOrder(orderId);
    std::cout << orderbook.Size() << std::endl; // 0
    return 0;
}