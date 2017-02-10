# -*- coding:utf-8 -*-

from CloudQuant import MiniSimulator
import numpy as np
import pandas as pd

username = 'Harvey_Sun'
password = 'P948894dgmcsy'
Strategy_Name = 'TurtleTrading_20_10'

INIT_CAP = 100000000
START_DATE = '20130101'
END_DATE = '20161231'
fee_rate = 0.001

D1 = 20  # 短线系统，20日ATR，买入均线为D1日最高
Ds = 10  # 短线系统，卖出均线为Ds日最低
D2 = 55  # 长线系统
num = 50  # 买入多少只股票
unit_limit = 4.

def initial(sdk):
    sdk.prepareData(['LZ_GPA_QUOTE_THIGH', 'LZ_GPA_QUOTE_TLOW', 'LZ_GPA_QUOTE_TCLOSE',
                     'LZ_GPA_VAL_A_TCAP', 'LZ_GPA_SLCIND_STOP_FLAG', 'LZ_GPA_SLCIND_ST_FLAG'])

    stock_list = sdk.getStockList()
    close = pd.DataFrame(sdk.getFieldData('LZ_GPA_QUOTE_TCLOSE')[-63:], columns=stock_list)
    not_new = pd.notnull(close).all(axis=0)


    st = pd.Series(sdk.getFieldData('LZ_GPA_SLCIND_ST_FLAG')[-1], index=stock_list)
    not_st = pd.isnull(st)

    not_new_stocks = list(close.columns[not_new])
    not_st_stocks = list(st.index[not_st])
    not_new_not_st = list(set(not_new_stocks) & set(not_st_stocks))
    cap = pd.Series(sdk.getFieldData('LZ_GPA_VAL_A_TCAP')[-1], index=stock_list)[not_new_not_st]

    cap.sort_values(inplace=True)
    stock_pool = list(cap[:num].index)
    sdk.setGlobal('stock_pool', stock_pool)

    high = pd.DataFrame(sdk.getFieldData('LZ_GPA_QUOTE_THIGH')[-(D1 + 2):-1], columns=stock_list)[stock_pool]
    low = pd.DataFrame(sdk.getFieldData('LZ_GPA_QUOTE_TLOW')[-(D1 + 2):-1], columns=stock_list)[stock_pool]
    close = pd.DataFrame(sdk.getFieldData('LZ_GPA_QUOTE_TCLOSE')[-(D1 + 2):-1], columns=stock_list)[stock_pool]

    x1 = (high - low)[1:]
    x2 = np.abs(high - close.shift(1))[1:]
    x3 = np.abs(low - close.shift(1))[1:]
    max23 = np.where(x2 > x3, x2, x3)
    tr = np.where(x1 > max23, x1, max23)
    atr = pd.Series(tr.mean(axis=0), index=stock_pool)
    sdk.setGlobal('atr', atr)

    stock_position = dict([i, 0] for i in stock_pool)
    buy_prices = dict([i, 0] for i in stock_pool)
    sdk.setGlobal('stock_position', stock_position)
    sdk.setGlobal('buy_prices', buy_prices)


def strategy(sdk):
    sdk.sdklog(sdk.getNowDate(), '================================')
    stock_list = sdk.getStockList()
    not_stop_stocks = pd.Series(stock_list)[pd.isnull(sdk.getFieldData('LZ_GPA_SLCIND_STOP_FLAG')[-(D1 + 1):]).all(axis=0)]
    stock_pool = sdk.getGlobal('stock_pool')
    tradable_stocks = set(stock_pool) & set(not_stop_stocks)

    quotes = sdk.getQuotes(tradable_stocks)

    high = pd.DataFrame(sdk.getFieldData('LZ_GPA_QUOTE_THIGH')[-(D1 + 1):], columns=stock_list)[stock_pool]
    low = pd.DataFrame(sdk.getFieldData('LZ_GPA_QUOTE_TLOW')[-(D1 + 1):], columns=stock_list)[stock_pool]
    close = pd.DataFrame(sdk.getFieldData('LZ_GPA_QUOTE_TCLOSE')[-(D1 + 1):], columns=stock_list)[stock_pool]
    x1 = (high - low).iloc[-1]
    x2 = np.abs(high - close.shift(1)).iloc[-1]
    x3 = np.abs(low - close.shift(1)).iloc[-1]
    max23 = np.where(x2 > x3, x2, x3)
    tr = pd.Series(np.where(x1 > max23, x1, max23), index=stock_pool)

    atr_pre = sdk.getGlobal('atr')
    atr = ((D1 - 1) * atr_pre + tr) / D1

    max_high = high[:-1].max()
    min_low = (low[-(Ds + 1):-1].min() + max_high) / 2

    net_value = sdk.getAccountInfo().previousAsset
    unit = np.floor((net_value / num) * 0.01 / (atr * 100))  # 一单位，手
    available_cash = sdk.getAccountInfo().availableCash
    stock_position = sdk.getGlobal('stock_position')
    buy_prices = sdk.getGlobal('buy_prices')
    available_cash_one_stock = available_cash / (num - sum(stock_position.values()) / unit_limit)

    positions = sdk.getPositions()
    position_dict = dict([i.code, i.optPosition] for i in positions)

    orders = []
    for stock in tradable_stocks:
        today_open = quotes[stock].open
        buy_volume = unit[stock] * 100
        money_needed = buy_volume * today_open * (1 + fee_rate)

        if (stock_position[stock] == 0) & (close.iloc[-1][stock] > max_high[stock]) & (money_needed <= available_cash_one_stock):
            order = [stock, today_open, buy_volume, 1]
            orders.append(order)
            stock_position[stock] = 1
            buy_prices[stock] = today_open
        elif (0 < stock_position[stock] < 4) & (close.iloc[-1][stock] > buy_prices[stock] + 0.5 * atr[stock]) & (money_needed <= available_cash_one_stock):
            order = [stock, today_open, buy_volume, 1]
            orders.append(order)
            stock_position[stock] += 1
            buy_prices[stock] = today_open
        elif (stock_position[stock] > 0) & ((close.iloc[-1][stock] < min_low[stock]) | (close.iloc[-1][stock] < buy_prices[stock] - 2 * atr[stock])):
            sell_volume = position_dict[stock]
            order = [stock, today_open, sell_volume, -1]
            orders.append(order)
            stock_position[stock] = 0
        else:
            pass

    sdk.makeOrders(orders)
    sdk.sdklog('下单')
    sdk.sdklog(np.array(orders))

    sdk.setGlobal('atr', atr)
    sdk.setGlobal('stock_position', stock_position)
    sdk.setGlobal('buy_prices', buy_prices)


config = {
    'username': username,
    'password': password,
    'initCapital': INIT_CAP,
    'startDate': START_DATE,
    'endDate': END_DATE,
    'strategy': strategy,
    'initial': initial,
    'feeRate': fee_rate,
    'strategyName': Strategy_Name,
    'logfile': '%s.log' % Strategy_Name,
    'rootpath': 'C:/cStrategy/',
    'executeMode': 'D',
    'feeLimit': 5,
    'cycle': 1,
    'dealByVolume': True,
    'allowForTodayFactors': ['LZ_GPA_SLCIND_STOP_FLAG']
}


if __name__ == "__main__":
    # 在线运行所需代码
    import os
    config['strategyID'] = os.path.splitext(os.path.split(__file__)[1])[0]
    MiniSimulator(**config).run()