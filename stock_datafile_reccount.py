# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Author Ramesh, Created Aug 27, 2019
# Pre-Process Step
# Program to count number of records for various stock datasets
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


#phd_data_dir = phd_dir + "data/stock/processed/"
#phd_data_dir = phd_dir + "data/stock/"


phd_dir = "C:/My Cloud/GoogleDrive/1 PhD/"
phd_data_dir = phd_dir + "data/"
proj_dir = phd_dir + "apds/stock/"
proj_data_dir = phd_data_dir + "stock/"
source_data_dir = phd_dir + "data/stock/" + time_interval + "/" + stock_etf + "/"

source_data_files = []
date_format_str = '%Y-%m-%d'  # The format

# Create list of files to be processed
for root, dirs, files in os.walk(source_data_dir):
    for filename in files:
        source_data_files.append(filename)
        #print(filename)

stock_trade_symbol_filename = proj_data_dir + stock_etf + "_trade_" + time_interval + "_symbols.csv"
stock_trade_symbol_file = open(stock_trade_symbol_filename, "w")
stock_trade_symbol_file.write("Stock Symbol, Frequency Count\n")

stock_trade_date_filename   = proj_data_dir + stock_etf + "_trade_" + time_interval + "_dates.csv"
stock_trade_date_file       = open(stock_trade_date_filename, "w")
stock_trade_date_file.write("Trade Date, Frequency Count\n")

stock_trade_record_count = 0
stock_trade_symbol_count = 0
stock_trade_date_count = 0
source_file_count = 0

stock_trade_symbol_dist = {}
stock_trade_date_dist = {}

for source_data_file in source_data_files:
    source_file_count = source_file_count + 1
    #if source_file_count > 10: break
    source_data_filename = source_data_dir + source_data_file
    print('source_data_filename',source_data_filename)

    source_data_file_fields = source_data_file.split('.')
    stock_trade_symbol = source_data_file_fields[0].strip()

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

        if stock_trade_symbol in stock_trade_symbol_dist.keys():
            stock_trade_symbol_freq = stock_trade_symbol_dist[stock_trade_symbol] + 1
        else:
            stock_trade_symbol_freq = 1
        stock_trade_symbol_dist.update({stock_trade_symbol: stock_trade_symbol_freq})

        #print('stock_trade_symbol',stock_trade_symbol,'stock_trade_symbol_freq',str(stock_trade_symbol_freq),'stock_trade_symbol_dist')

        stock_trade_date = stock_trade_line[0].strip()
        if stock_trade_date in stock_trade_date_dist.keys():
            stock_trade_date_freq = stock_trade_date_dist[stock_trade_date] + 1
        else:
            stock_trade_date_freq = 1
        stock_trade_date_dist.update({stock_trade_date: stock_trade_date_freq})
        #print(stock_trade_date,stock_trade_date_freq)
    stock_trade_file.close()

for stock_trade_symbol in list(stock_trade_symbol_dist.keys()):
    stock_trade_symbol_freq = stock_trade_symbol_dist[stock_trade_symbol]
    stock_trade_symbol_file.write(stock_trade_symbol + "," + str(stock_trade_symbol_freq) + "\n")

for stock_trade_date in list(stock_trade_date_dist.keys()):
    stock_trade_date_freq = stock_trade_date_dist[stock_trade_date]
    stock_trade_date_file.write(str(stock_trade_date) + "," + str(stock_trade_date_freq) + "\n")

stock_trade_symbol_file.close()
stock_trade_date_file.close()

current_time = datetime.datetime.now()
prg_endtime = time.process_time()
prg_endtime2 = time.asctime(time.localtime(time.time()))
print ('Program End Time: ' + str(current_time.strftime("%x")) + ' ' + str((current_time.strftime("%X"))) + " Run Time : " + str(prg_endtime - prg_starttime) + " Seconds")

#end of the program