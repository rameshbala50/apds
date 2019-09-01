# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Author Ramesh, Created Aug 26, 2019
# Phase-I CMS Workings
# Program to create sketch and analysis with Stock Data
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

import time;
import datetime;
import csv
import os
from probables import (CountMinSketch)

timenow = datetime.datetime.now()
dateval = timenow.strftime("%y") + timenow.strftime("%m") + timenow.strftime("%d") + timenow.strftime("%H") + timenow.strftime("%M") + timenow.strftime("%S")
process_starttime = time.process_time()
prg_starttime = time.asctime(time.localtime(time.time()))
print ('Program Start Time: ' + prg_starttime)

#time_interval = "5min"
#time_interval = "hourly"
time_interval = "daily"

stock_etf = "stock"
#stock_etf = "etf"

#no_of_records = [10000000]
no_of_records = [1000,2000,3000]
#no_of_records = [2000000,4000000,6000000,8000000,10000000]
#no_of_records = [10000,20000,30000,40000,50000,60000,70000,80000,90000,100000]
#no_of_records = [100000,200000,300000,400000,500000,600000,700000,800000,900000,1000000]
#no_of_records = [1000000,2000000,3000000,4000000,5000000,6000000,7000000]

widths = [1000]
#widths = [1000,2000,3000,4000,5000,6000,7000,8000,9000,10000]

depths = [4]
#depths = [2,4,6,8,10]

phd_dir = "C:/My Cloud/GoogleDrive/1 PhD/"
phd_data_dir = phd_dir + "data/"
proj_dir = phd_dir + "apds/stock/"
proj_data_dir = phd_data_dir + "stock/"

process_filename = proj_dir + stock_etf + time_interval  + "_cms_process_" + str(dateval) + ".csv"
process_file = open(process_filename, "w")

for no_of_record in no_of_records:
    for depth in depths:
        process_file.write('No of Input Records, Distinct Input Count, Sketch Width, No of Hash Fn')
        process_file.write(', Accuracy, Sketch Size(KB)')
        process_file.write(',CMS Create Time, CMS Qry Time, Prg Start Time, Prg End Time\n')

        for width in widths:
            timenow = datetime.datetime.now()
            cms_starttime =  str(timenow.strftime("%x"))+ ' ' + str((timenow.strftime("%X")))
            #stock_trade_filename = proj_data_dir + stock_etf + "_trade_" + time_interval + "_R" + str(no_of_record) + ".csv"
            stock_trade_filename = proj_data_dir + stock_etf + "_trade_" + time_interval + "_R15000000.csv"
            stock_trade_file = open(stock_trade_filename,"r")
            stock_trade_lines = csv.reader(stock_trade_file, delimiter=',', quoting=csv.QUOTE_ALL, skipinitialspace=True)
            next(stock_trade_lines)
            print('No of Input Rec:', str(no_of_record),' Sketch Width:', str(width),' Depth:', str(depth), ' CMS Start Time:', cms_starttime)

            stock_symbol_filename = proj_dir + "stock_freq_files/" + stock_etf + time_interval + "_R" + str(no_of_record) + "_w" + str(width) + "_d" + str(depth) + "_freq.csv"
            stock_symbol_file = open(stock_symbol_filename, "w")
            stock_symbol_file.write('Stock Symbol, No of Input Records, Sketch Width, Sketch Depth, Actual Trade Count, CMS Trade Count, Accuracy\n')

            cms_filename = proj_dir + "cms_files/" + stock_etf + time_interval + "_R" + str(no_of_record) + "_w" + str(width) + "_d" + str(depth) + "_freq.cms"

            stock_symbol_dist = {}
            stock_freq_cms = CountMinSketch(width, depth)
            stock_trade_record_count = 0
            sketch_time = 0
            sketch_qrytime = 0
            total_accuracy = 0

            for stock_trade_line in stock_trade_lines:
                stock_trade_record_count = stock_trade_record_count + 1
                if stock_trade_record_count > no_of_record: break
                stock_symbol = stock_trade_line[0].strip()

                sketch_starttime = time.process_time()
                add1 = stock_freq_cms.add(stock_symbol)
                sketch_endtime = time.process_time()
                sketch_time = sketch_time + (sketch_endtime - sketch_starttime)

                if stock_symbol in stock_symbol_dist.keys():
                    stock_symbol_freq = stock_symbol_dist[stock_symbol] + 1
                else:
                    stock_symbol_freq = 1

                stock_symbol_dist.update({stock_symbol: stock_symbol_freq})

            for stock_symbol in list(stock_symbol_dist.keys()):
                sketch_starttime = time.process_time()
                stock_symbol_freq_cms = stock_freq_cms.check(stock_symbol)
                sketch_endtime = time.process_time()
                sketch_qrytime = sketch_qrytime + (sketch_endtime - sketch_starttime)

                stock_symbol_freq = stock_symbol_dist[stock_symbol]
                accuracy = 1 - abs(stock_symbol_freq-stock_symbol_freq_cms)/stock_symbol_freq
                if accuracy < 0: accuracy = 0
                total_accuracy = total_accuracy + accuracy
                stock_symbol_file.write(stock_symbol + "," + str(no_of_record))
                stock_symbol_file.write("," + str(width) + "," + str(depth))
                stock_symbol_file.write("," + str(stock_symbol_freq) + "," + str(stock_symbol_freq_cms))
                stock_symbol_file.write("," + str(accuracy) + "\n")

            symbol_count = len(stock_symbol_dist)
            avg_accuracy = total_accuracy / symbol_count
            stock_freq_cms.export(cms_filename)
            stock_freq_cms.clear()

            stock_symbol_file.write("Averge," + str(no_of_record))
            stock_symbol_file.write("," + str(width) + "," + str(depth))
            stock_symbol_file.write(",,," + str(avg_accuracy) + "\n")

            timenow = datetime.datetime.now()
            cms_endtime = str(timenow.strftime("%x")) + ' ' + str((timenow.strftime("%X")))

            process_file.write(str(no_of_record) + "," + str(symbol_count) + "," + str(width) + "," + str(depth))
            process_file.write("," + str(avg_accuracy) + "," + str(os.path.getsize(cms_filename)/1024))
            process_file.write("," + str(sketch_time) + "," + str(sketch_qrytime) + "," + cms_starttime + "," + cms_endtime + "\n")

            stock_trade_file.close()
            stock_symbol_file.close()

        process_file.write("\n")

process_file.close()

process_endtime = time.process_time()
prg_endtime = time.asctime(time.localtime(time.time()))
print('Program Start Time: ' + prg_starttime + " Run Time : " + str(process_endtime - process_starttime) + " End Time: " + prg_endtime)

# end of the program