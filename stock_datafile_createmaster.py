# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Author Ramesh, Created Jul 12, 2019
# Pre-Process Step
# Program to count/list of dates, stock symbols in the source data files (a.us, aapl.us, etc.)
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

import time;
import datetime
import csv
import os

current_time = datetime.datetime.now()
dateval = current_time.strftime("%y") + current_time.strftime("%d") + current_time.strftime("%m") + current_time.strftime("%H") + current_time.strftime("%M") + current_time.strftime("%S")
prg_starttime = time.process_time()
print ('Program Start Time: ' + str(current_time.strftime("%x")) + ' ' + str((current_time.strftime("%X"))))

time_interval = "5min"
#time_interval = "hourly"
#time_interval = "daily"

stock_etf = "stock"
#stock_etf = "etf"

#no_of_records = [1000,2000,3000]
#no_of_records = [1000,2000,3000,4000,5000,6000,7000,8000,9000]
no_of_records = [1000000,2000000,3000000,4000000,5000000,6000000,7000000]

phd_dir = "C:/My Cloud/GoogleDrive/1 PhD/"
source_data_dir = phd_dir + "data/stock/" + time_interval + "/" + stock_etf + "/"
phd_data_dir = phd_dir + "data/stock/preprocessed/"
#phd_data_dir = phd_dir + "data/stock/"

source_data_files = []
date_format_str = '%Y-%m-%d'  # The format

# Create list of files to be processed
for root, dirs, files in os.walk(source_data_dir):
    for filename in files:
        source_data_files.append(filename)
        #print(filename)

for no_of_record in no_of_records:
    #stock_trade_filename = phd_data_dir + stock_etf + "_trade_" + time_interval + "_R" + str(no_of_record) + ".csv"
    #print('Input Data File:',stock_trade_filename)

    stock_trade_symbols_filename = phd_data_dir + stock_etf + "_trade_" + time_interval + "_R" + str(no_of_record) + "_symbols.csv"
    stock_trade_symbols_file = open(stock_trade_symbols_filename, "w")
    stock_trade_symbols_file.write("stock symbol\n")

    stock_trade_dates_filename   = phd_data_dir + stock_etf + "_trade_" + time_interval + "_R" + str(no_of_record) + "_dates.csv"
    stock_trade_dates_file   = open(stock_trade_dates_filename, "w")
    stock_trade_dates_file.write("trade date\n")

    stock_trade_record_count = 0
    stock_trade_symbol_count = 0

    stock_trade_date_count = 0
    stock_trade_dates = []

    for source_data_file in source_data_files:
        if stock_trade_record_count >= no_of_record: break
        source_data_filename = source_data_dir + source_data_file

        #print('Input Data File:',source_data_file)

        source_data_file_fields = source_data_file.split('.')
        stock_trade_symbol = source_data_file_fields[0].strip()
        stock_trade_symbols_file.write(stock_trade_symbol + "\n")
        stock_trade_symbol_count = stock_trade_symbol_count + 1

        stock_trade_file = open(source_data_filename, "r")
        stock_trade_lines = csv.reader(stock_trade_file, delimiter=',', quoting=csv.QUOTE_ALL, skipinitialspace=True)
        source_filesize = os.path.getsize(source_data_filename)

        if source_filesize <= 0:print('Empty File (0 file size): ', source_data_filename, source_filesize)
        else:header = next(stock_trade_lines)

        if source_data_file == "desktop.ini":
            print('Skipping desktop.ini file:', source_data_filename, source_filesize)
            continue

        for stock_trade_line in stock_trade_lines:
            stock_trade_record_count = stock_trade_record_count + 1
            if stock_trade_record_count >= no_of_record: break

            stock_trade_date = stock_trade_line[0].strip()
            if stock_trade_date not in stock_trade_dates:
                stock_trade_dates.append(stock_trade_date)

        stock_trade_dates.sort()
        for stock_trade_date in stock_trade_dates:
            stock_trade_dates_file.write(stock_trade_date + "\n")
            stock_trade_date_count = stock_trade_date_count + 1

        stock_trade_file.close()

    stock_trade_symbols_file.close()
    stock_trade_dates_file.close()
    print("No of Trade Records: " + str(stock_trade_record_count) + " No. of Stocks: " + str(stock_trade_symbol_count) + " No. of Dates: " + str(stock_trade_date_count))

current_time = datetime.datetime.now()
prg_endtime = time.process_time()
prg_endtime2 = time.asctime(time.localtime(time.time()))
print ('Program End Time: ' + str(current_time.strftime("%x")) + ' ' + str((current_time.strftime("%X"))) + " Run Time : " + str(prg_endtime - prg_starttime) + " Seconds")

#end of the program