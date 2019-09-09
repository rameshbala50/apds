# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Author Ramesh, Created Jul 12, 2019
# Pre-Process Step
#Program to create data file for time (like date1, date2, date3,...,etc) using source data files (a.us, aapl.us, etc.)
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

import time
import datetime
import os
import csv

from datetime import timedelta

current_time = datetime.datetime.now()
prg_starttime = time.process_time()
print ('Program Start Time: ' + str(current_time.strftime("%x")) + ' ' + str((current_time.strftime("%X"))))

time_interval = "5min"
#time_interval = "hourly"
#time_interval = "daily"

stock_etf = "stock"
#stock_etf = "etf"

start_date = datetime.date(2017,2,1)
end_date = datetime.date(2017,3,31)
day = datetime.timedelta(days=1)

phd_dir = "C:/My Cloud/GoogleDrive/1 PhD/"
phd_data_dir = phd_dir + "data/"
proj_dir = phd_dir + "apds/stock/"
proj_data_dir = phd_data_dir + "stock/"
source_data_dir = phd_dir + "data/stock/" + time_interval + "/" + stock_etf + "/"

data_files = []
trade_dates = []
file_created_dates = []
date_format_str = '%Y-%m-%d'  # The format
stock_symbol_count = 0

for root, dirs, files in os.walk(source_data_dir):
    for filename in files:
        data_files.append(filename)

while start_date <= end_date:
    trade_dates.append(start_date)
    start_date = start_date + day

for count in range(0,len(trade_dates)):
    trade_date = trade_dates[count]
    create_file = "YES"
    file_created = "NO"
    stock_trade_rec_count = 0

    for data_file in data_files:
        stock_symbol_count = stock_symbol_count + 1
        stock_filename = source_data_dir + data_file

        data_file_fields = data_file.split('.')
        stock_symbol = data_file_fields[0].strip()

        stock_file = open(stock_filename, "r")
        stock_lines = csv.reader(stock_file, delimiter=',', quoting=csv.QUOTE_ALL, skipinitialspace=True)
        filesize = os.path.getsize(stock_filename)
        if filesize > 0: next(stock_file)

        for stock_line in stock_lines:
            stock_trade_rec_count = stock_trade_rec_count + 1
            stock_trade_date = str(stock_line[0]).strip()
            if time_interval == "5min" or time_interval == "hourly":
                stock_trade_time = str(stock_line[1]).strip()
                stock_open_price = str(stock_line[2]).strip()
                stock_high_price = str(stock_line[3]).strip()
                stock_low_price = str(stock_line[4]).strip()
                stock_close_price = str(stock_line[5]).strip()
                stock_volume_price = str(stock_line[6]).strip()
                stock_openinterest_price = str(stock_line[7]).strip()
            else:
                stock_trade_time = ""
                stock_open_price = str(stock_line[1]).strip()
                stock_high_price = str(stock_line[2]).strip()
                stock_low_price = str(stock_line[3]).strip()
                stock_close_price = str(stock_line[4]).strip()
                stock_volume_price = str(stock_line[5]).strip()
                stock_openinterest_price = str(stock_line[6]).strip()

            stock_trade_date_obj = datetime.datetime.strptime(stock_trade_date, date_format_str)
            stock_trade_year = stock_trade_date_obj.year

            if str(stock_trade_date) == str(trade_date):
                if create_file == "YES":
                    file_created = "YES"
                    stock_trade_filename = proj_data_dir + "time/" + stock_etf + time_interval + "_trade_" + str(trade_date) + '.csv'
                    stock_trade_file = open(stock_trade_filename, "w")
                    stock_trade_file.write('stock_symbol, trade_date, trade_time, open price, high, low, close price, volume, open interest, trade year\n')
                    create_file = "NO"
                    print(stock_trade_filename, trade_date)
                stock_trade_file.write(stock_symbol + ',' + stock_trade_date + ',' + stock_trade_time)
                stock_trade_file.write(',' + stock_open_price + ',' + stock_high_price)
                stock_trade_file.write(',' + stock_low_price + ',' + stock_close_price + ',' + stock_volume_price)
                stock_trade_file.write(',' + stock_openinterest_price + ',' + str(stock_trade_year) + "\n")

    if file_created == "YES":
        stock_trade_file.close()
        current_time = datetime.datetime.now()
        print("File Created - Stock Trade Records: " + str(stock_trade_rec_count) + " Trade Date: " + str(trade_date) + " Current Time: " + str(current_time.strftime("%x")) + ' ' + str((current_time.strftime("%X"))))

current_time = datetime.datetime.now()
prg_endtime = time.process_time()
print('Program End Time: ' + str(current_time.strftime("%x")) + ' ' + str((current_time.strftime("%X"))) + " Run Time : " + str(prg_endtime - prg_starttime) + " Seconds")

#end of the program
