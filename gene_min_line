import tickspider
import tushare as ts
import pandas as pd
import datetime
import os
from multiprocessing import Pool

tick_data_dir = '/home/prod/data/stock_tick_data'
min_data_dir = '/home/prod/data/stock_min_data'


def gen_min_line(symbol, date):
    global tick_data_dir
    str_date = str(date)
    tick_dir = tick_data_dir + '/' + symbol + \
        '/' + str(date.year) + '/' + str(date.month)
    min_dir = min_data_dir + '/' + symbol + \
        '/' + str(date.year) + '/' + str(date.month)
    tickfile = tick_dir + '/' + symbol + '_' + str_date + '_tick_data.h5'
    minfile = min_data_dir + '/' + symbol + '_' + str_date + '_1min_data.h5'
    if (os.path.exists(tickfile)) and (not os.path.exists(minfile)):
        if tickspider.is_open_day(str_date):
            if not os.path.exists(min_dir):
                os.makedirs(min_dir)
            hdf5_file = pd.HDFStore(tickfile, 'r')
            df = hdf5_file['data']
            hdf5_file.close()
            print("Successfully read tick file: " + tickfile)
            # TuShare即便在停牌期间也会返回tick data，并且只有三行错误的数据，这里利用行数小于10把那些unexpected
            # tickdata数据排除掉
            df=df.dropna()
            if df.shape[0] < 10:
                print("No tick data read from tick file, skip generating 1min line")
                return 0
            df['time'] = str_date + ' ' + df['time']
            df['time'] = pd.to_datetime(df['time'])
            df = df.set_index('time')
            price_df = df['price'].resample('1min').ohlc()
            price_df = price_df.dropna()
            vols = df['volume'].resample('1min').sum()
            vols = vols.dropna()
            vol_df = pd.DataFrame(vols, columns=['volume'])
            amounts = df['amount'].resample('1min').sum()
            amounts = amounts.dropna()
            amount_df = pd.DataFrame(amounts, columns=['amount'])
            newdf = price_df.merge(vol_df, left_index=True, right_index=True).merge(
                amount_df, left_index=True, right_index=True)
            hdf5_file2 = pd.HDFStore(minfile, 'w')
            hdf5_file2['data'] = newdf
            hdf5_file2.close()
            print("Successfully write to minute file: " + minfile)

dates = tickspider.get_date_list(
    datetime.date(2017, 8, 1), datetime.date.today())
stocks = tickspider.get_all_stock_id()


pool = Pool(32)
tasks = [(stock,date) for stock in stocks for date in dates]
pool.starmap(gen_min_line,tasks)
pool.close()

#for stock in stocks:
#    for date in dates:
#        gen_min_line(stock, date)
