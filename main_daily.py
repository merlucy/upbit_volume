from upbit import Upbit
import pandas as pd
import time, os, pytz
from datetime import datetime, timedelta
import matplotlib.pyplot as plt

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
        market_data = upbit.get_candles_day(market=market['market'], to=cur_time_in_iso, count=200).json()
        market_data_2 = upbit.get_candles_day(market=market['market'], to=cur_time_in_iso_2, count=200).json()

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

        df.to_parquet(data_file_path + '/' + market['market'] + '_daily.parquet', index=False)

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

get_data(upbit, krw_markets)

final_df = pd.DataFrame()
target_cols = [
    'candle_date_time_utc',
    'candle_acc_trade_price',
    'candle_acc_trade_volume',
]

index_col = []
val_col = []

for market in krw_markets:
    dir_path = os.getcwd()
    data_file_path = dir_path + '/data_files'

    df_2 = pd.read_parquet(path=data_file_path + '/' + market['market'] + '_daily.parquet')

    if market['market'] == 'KRW-BTC':
        df_2['candle_date_time_utc'] = df_2['candle_date_time_utc'].apply(lambda x: x[:-9])
        index_col = list(df_2['candle_date_time_utc'].values)
        val_col = list(df_2['candle_acc_trade_price'].values)
        continue

    raw_vals = list(df_2['candle_acc_trade_price'].values)

    for i in range(len(raw_vals)):
        val_col[i] = val_col[i] + raw_vals[i]

final_df = pd.DataFrame()
final_df['date'] = index_col
final_df['trade_vol'] = val_col
final_df['trade_vol'] = final_df['trade_vol'] / 1_000_000_000
final_df = final_df.iloc[::-1]
final_df = final_df.iloc[-400:]

final_df['date'] = pd.to_datetime(final_df['date'])

print(final_df.head(5))
print(final_df.tail(5))

plt_data = final_df[['date', 'trade_vol']]
plt_data.set_index(['date'], inplace=True)
plt_data = plt_data.plot().get_figure()
plt_data.savefig('img_daily.png')
#print(upbit.get_candles_week(market='KRW-SEI', count=200).json())