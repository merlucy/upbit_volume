from upbit import Upbit
import pandas as pd
import time, os, pytz
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
from tabulate import tabulate

def get_data(api:Upbit, markets):

    dir_path = os.getcwd()
    data_file_path = dir_path + '/data_files'

    counter = 0

    cur_time = time.time()
    tz = pytz.timezone('UTC')
    #tz = pytz.timezone('Asia/Seoul')

    end_t_1 = datetime.fromtimestamp(cur_time, tz)
    end_t_2 = datetime.fromtimestamp(cur_time, tz) - timedelta(weeks=200)

    cur_time_in_iso = str(end_t_1.isoformat()[:-13])
    cur_time_in_iso_2 = str(end_t_2.isoformat()[:-13])

    print(cur_time_in_iso)
    print(cur_time_in_iso_2)

    #print(cur_time_in_iso)

    #cur_time_in_iso = datetime.utcfromtimestamp(time.time()).strftime('%Y-%m-%dT%H-%M-%SZ')

    for market in markets:
        market_data = upbit.get_candles_week(market=market['market'], to=cur_time_in_iso, count=200).json()
        market_data_2 = upbit.get_candles_week(market=market['market'], to=cur_time_in_iso_2, count=200).json()

        columns = list(market_data[-1].keys())
        data_dict = dict()

        df = pd.DataFrame()
        
        for week_data in market_data:
            for k in columns:
                if not k in data_dict:
                    data_dict[k] = list()

                data_dict[k].append(week_data[k])

        for week_data in market_data_2:
            for k in columns:
                if not k in data_dict:
                    data_dict[k] = list()

                data_dict[k].append(week_data[k])

        for k in data_dict:
            df[k] = data_dict[k]

        df.to_parquet(data_file_path + '/' + market['market'] + '_weekly.parquet', index=False)

        time.sleep(0.2)

        #counter += 1
        #if counter % 9 == 0:
        #    break
        #    counter = 0

upbit = Upbit()
res = upbit.get_markets()
markets = res.json()

btc_markets = []
krw_markets = []

for market in markets:
    print(market)
    if market['market'][:3] == 'KRW':
        krw_markets.append(market)
    else:
        btc_markets.append(market)

print(len(krw_markets))
print(len(btc_markets))

#get_data(api=upbit, markets=krw_markets)

final_df = pd.DataFrame()
target_cols = [
    'candle_date_time_utc',
    'candle_acc_trade_price',
    'candle_acc_trade_volume',
]

index_col = []
val_col = []

dir_path = os.getcwd()
data_file_path = dir_path + '/data_files'

df_btc = ''

for market in krw_markets:
    df_2 = pd.read_parquet(path=data_file_path + '/' + market['market'] + '_weekly.parquet')

    if market['market'] == 'KRW-BTC':
        df_2['candle_date_time_utc'] = df_2['candle_date_time_utc'].apply(lambda x: x[:-9])
        index_col = list(df_2['candle_date_time_utc'].values)
        val_col = [0]*len(df_2['candle_acc_trade_price'].values)
        print(val_col)
        df_btc = df_2.copy()
        continue

    raw_vals = list(df_2['candle_acc_trade_price'].values)

    for i in range(len(raw_vals)):
        val_col[i] = val_col[i] + raw_vals[i]

final_df = pd.DataFrame()
final_df['date'] = index_col
final_df['trade_vol'] = val_col
final_df['date'] = final_df['date']
final_df['trade_vol'] = final_df['trade_vol']/1_000_000_000

final_df = final_df.iloc[::-1]
final_df = final_df.iloc[-200:]

final_df['date'] = pd.to_datetime(final_df['date'])

vol_2020 = final_df.loc[(final_df['date'] >= '2020-01-01') & (final_df['date'] < '2021-01-01')]['trade_vol'].sum()
vol_2021 = final_df.loc[(final_df['date'] >= '2021-01-01') & (final_df['date'] < '2022-01-01')]['trade_vol'].sum()
vol_2022 = final_df.loc[(final_df['date'] >= '2022-01-01') & (final_df['date'] < '2023-01-01')]['trade_vol'].sum()
vol_2023 = final_df.loc[(final_df['date'] >= '2023-01-01') & (final_df['date'] < '2024-01-01')]['trade_vol'].sum()
vol_2024 = final_df.loc[(final_df['date'] >= '2024-01-01') & (final_df['date'] < '2025-01-01')]['trade_vol'].sum()

plt_data = final_df[['date', 'trade_vol']]
plt_data.set_index(['date'], inplace=True)
plt_data = plt_data.plot().get_figure()
plt_data.savefig('img.png')

btc_final_df = pd.DataFrame()
btc_final_df['date'] = df_btc['candle_date_time_utc']
btc_final_df['trade_vol'] = df_btc['candle_acc_trade_price']/1_000_000_000
btc_final_df = btc_final_df.iloc[::-1]
btc_final_df = btc_final_df.iloc[-200:]

plt_data = btc_final_df[['date', 'trade_vol']]
plt_data.set_index(['date'], inplace=True)
plt_data = plt_data.plot().get_figure()
plt_data.savefig('img_btc.png')
#print(upbit.get_candles_week(market='KRW-SEI', count=200).json())

print(tabulate([
    ['2020', round(vol_2020)],
    ['2021', round(vol_2021)],
    ['2022', round(vol_2022)],
    ['2023', round(vol_2023)],
    ['2024', round(vol_2024)],
    ]))