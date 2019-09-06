# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Author Ramesh, Created Jul 12, 2019
# Pre-Process Step
# Program to create data file for 1M, 2M, 3M. etc. using source data files (a.us, aapl.us, etc.)
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

import time
import datetime
import os
import csv

current_time = datetime.datetime.now()
prg_starttime = time.process_time()
print ('Program Start Time: ' + str(current_time.strftime("%x")) + ' ' + str((current_time.strftime("%X"))))

#time_interval = "5min"
#time_interval = "hourly"
time_interval = "daily"

stock_etf = "stock"
#stock_etf = "etf"

no_of_records = [100]
#no_of_records = [15000000]

phd_dir = "C:/My Cloud/GoogleDrive/1 PhD/"
phd_data_dir = phd_dir + "data/"
proj_dir = phd_dir + "apds/stock/"
proj_data_dir = phd_data_dir + "stock/"
source_data_dir = phd_dir + "data/stock/" + time_interval + "/" + stock_etf + "/"

data_files = []
date_format_str = '%Y-%m-%d'  # The format
stocks_to_filter = ['aapl', 'ibm', 'msft']

# Create list of files to be processed
for root, dirs, files in os.walk(source_data_dir):
    for filename in files:
        data_files.append(filename)
        #print(filename)

for no_of_record in no_of_records:
    stock_trade_filename = proj_data_dir + stock_etf + "_trade_" + time_interval + "_R" + str(no_of_record) + ".csv"
    stock_trade_file = open(stock_trade_filename, "w")
    stock_trade_file.write('stock_symbol, trade_date, trade_time, open price, high, low, close price, volume, open interest\n')

    print('stock_trade_filename',stock_trade_filename)

    stock_trade_record_count = 0
    for data_file in data_files:
        if stock_trade_record_count >= no_of_record: break
        stock_filename = source_data_dir + data_file
        data_file_fields = data_file.split('.')
        stock_symbol = data_file_fields[0].strip()

        #print('Processing Filename (to add trade data):', stock_filename)
        stock_file = open(stock_filename, "r")
        stock_lines = csv.reader(stock_file, delimiter=',', quoting=csv.QUOTE_ALL, skipinitialspace=True)
        filesize = os.path.getsize(stock_filename)

        if filesize <= 0:print('Empty File (0 file size): ', stock_filename, filesize)
        else:header = next(stock_lines)
        #print(source_data_dir + "desktop.ini")
        if data_file == "desktop.ini":
            print('Skipping desktop.ini file:', stock_filename, filesize)
            continue

        for stock_line in stock_lines:
            if stock_trade_record_count >= no_of_record: break
            stock_trade_record_count = stock_trade_record_count + 1
            if (stock_trade_record_count % 100000) == 0: print("Records Processed:",stock_trade_record_count)

            stock_trade_date = str(stock_line[0]).strip()
            stock_trade_date_obj = datetime.datetime.strptime(stock_trade_date, date_format_str).date()
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

            #if stock_symbol not in stocks_to_filter: continue
            #if stock_trade_date != '2017-11-29': continue

            stock_trade_file.write(stock_symbol + ',' + stock_trade_date + ',' + stock_trade_time)
            stock_trade_file.write(',' + stock_open_price + ',' + stock_high_price)
            stock_trade_file.write(',' + stock_low_price + ',' + stock_close_price + ',' + stock_volume_price)
            stock_trade_file.write(',' + stock_openinterest_price + "\n")

    stock_trade_file.close()
    print('Stock Traded File Created:',stock_trade_filename,' Records Processed:',stock_trade_record_count)

current_time = datetime.datetime.now()
prg_endtime = time.process_time()
print('Program End Time: ' + str(current_time.strftime("%x")) + ' ' + str((current_time.strftime("%X"))) + " Run Time : " + str(prg_endtime - prg_starttime) + " Seconds")

#end of the program
