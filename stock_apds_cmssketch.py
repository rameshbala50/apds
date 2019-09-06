# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Author Ramesh, Created Sept 3, 2019
# Phase-III APDS Workings
# Program to create sketch for Stock Vol
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

import time;
import datetime;
import csv
import os
#from probables import (CountMinSketch)
from countminsketch import CountMinSketch

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

no_of_records = [8000000,9000000]
#no_of_records = [100]
#no_of_records = [1000000,2000000,3000000,4000000,5000000,6000000,7000000,8000000,9000000,10000000,11000000,12000000,13000000,14000000,15000000]

#widths = [1000]
widths = [1000,2000,3000,4000,5000,6000,7000,8000,9000,10000]

#depths = [2]
depths = [2,4,6,8,10]

phd_dir = "C:/My Cloud/GoogleDrive/1 PhD/"
phd_data_dir = phd_dir + "data/"
proj_dir = phd_dir + "apds/stock/"
proj_data_dir = phd_data_dir + "stock/"

process_filename = proj_dir + stock_etf + time_interval  + "_apds_apds_process_" + str(dateval) + ".csv"
process_file = open(process_filename, "w")

for no_of_record in no_of_records:
    for depth in depths:
        process_file.write('No of Input Records, Distinct Input Count, Sketch Width, No of Hash Fn')
        process_file.write(', Avg. Freq. Accuracy, Avg. Vol. Accuracy, Sketch Size(KB)')
        process_file.write(', Confidence, Error Rate')
        process_file.write(',APDS Create Time, APDS Qry Time, Prg Start Time, Prg End Time\n')

        for width in widths:
            timenow = datetime.datetime.now()
            apds_starttime =  str(timenow.strftime("%x"))+ ' ' + str((timenow.strftime("%X")))
            #stock_trade_filename = proj_data_dir + stock_etf + "_trade_" + time_interval + "_R" + str(no_of_record) + ".csv"
            stock_trade_filename = proj_data_dir + stock_etf + "_trade_" + time_interval + "_R15000000.csv"
            stock_trade_file = open(stock_trade_filename,"r")
            stock_trade_lines = csv.reader(stock_trade_file, delimiter=',', quoting=csv.QUOTE_ALL, skipinitialspace=True)
            next(stock_trade_lines)
            print('No of Input Rec:', str(no_of_record),' Sketch Width:', str(width),' Depth:', str(depth), ' APDS Start Time:', apds_starttime)

            stock_symbol_filename = proj_dir + "stock_vol_files/" + stock_etf + time_interval + "_R" + str(no_of_record) + "_w" + str(width) + "_d" + str(depth) + "_vol.csv"
            stock_symbol_file = open(stock_symbol_filename, "w")
            stock_symbol_file.write('Stock Symbol, No of Input Records, Sketch Width, Sketch Depth')
            stock_symbol_file.write(',Actual Trade Freq, APDS Trade Freq, Freq Accuracy')
            stock_symbol_file.write(',Actual Trade Volume, APDS Trade Volume, Vol Accuracy\n')

            #stock_input_filename = proj_dir + "stock_vol_files/" + stock_etf + time_interval + "_R" + str(no_of_record) + "_w" + str(width) + "_d" + str(depth) + "_input.csv"
            #stock_input_file = open(stock_input_filename, "w")
            #stock_input_file.write('Stock Symbol, Trade Date, Volume\n')

            apds_filename = proj_dir + "apds_files/" + stock_etf + time_interval + "_R" + str(no_of_record) + "_w" + str(width) + "_d" + str(depth) + "_freq.apds"

            stock_freq_dist = {}
            total_freq_accuracy = 0

            stock_vol_dist = {}
            total_vol_accuracy = 0

            stock_vol_apds = CountMinSketch(width, depth)
            stock_trade_record_count = 0
            vol_sketch_time = 0

            # add elements to sketch
            for stock_trade_line in stock_trade_lines:
                #print('stock_trade_record_count:',stock_trade_record_count,' no_of_record:',no_of_record)
                if stock_trade_record_count >= no_of_record: break
                stock_trade_record_count = stock_trade_record_count + 1
                stock_symbol = stock_trade_line[0].strip()
                trade_date = stock_trade_line[1].strip()
                stock_vol = int(stock_trade_line[7].strip())
                #stock_input_file.write(stock_symbol + "," + str(trade_date) + "," + str(stock_vol) + "\n")

                vol_sketch_starttime = time.process_time()
                apds_cmsadded = stock_vol_apds.add(stock_symbol,stock_vol)
                #print('stock_trade_record_count:',stock_trade_record_count, '  stock_symbol:',stock_symbol,'   stock_vol:',stock_vol,' add1:',apds_cmsadded)
                vol_sketch_endtime = time.process_time()
                vol_sketch_time = vol_sketch_time + (vol_sketch_endtime - vol_sketch_starttime)
                #print(vol_sketch_time,vol_sketch_endtime,vol_sketch_starttime)

                if stock_symbol in stock_vol_dist.keys():
                    stock_symbol_freq = stock_freq_dist[stock_symbol] + 1
                    stock_symbol_vol = stock_vol_dist[stock_symbol] + stock_vol
                else:
                    stock_symbol_freq = 1
                    stock_symbol_vol = stock_vol

                stock_freq_dist.update({stock_symbol: stock_symbol_freq})
                stock_vol_dist.update({stock_symbol: stock_symbol_vol})

            vol_sketch_qrytime = 0
            # read sketch
            for stock_symbol in list(stock_vol_dist.keys()):
                vol_sketch_starttime = time.process_time()
                stock_symbol_vol_apds = stock_vol_apds.check(stock_symbol)
                vol_sketch_endtime = time.process_time()
                vol_sketch_qrytime = vol_sketch_qrytime + (vol_sketch_endtime - vol_sketch_starttime)

                freq_accuracy = 0
                stock_symbol_freq = stock_freq_dist[stock_symbol]
                #freq_accuracy = 1 - abs(stock_symbol_freq-stock_freq_dist)/stock_symbol_freq
                if freq_accuracy < 0: freq_accuracy = 0
                total_freq_accuracy = total_freq_accuracy + freq_accuracy

                stock_symbol_vol = stock_vol_dist[stock_symbol]
                vol_accuracy = 1 - abs(stock_symbol_vol - stock_symbol_vol_apds)/stock_symbol_vol
                if vol_accuracy < 0: vol_accuracy = 0
                total_vol_accuracy = total_vol_accuracy + vol_accuracy

                stock_symbol_file.write(stock_symbol + "," + str(stock_trade_record_count))
                stock_symbol_file.write("," + str(width) + "," + str(depth))
                stock_symbol_file.write("," + str(stock_symbol_freq) + ",")
                stock_symbol_file.write("," + str(freq_accuracy))
                stock_symbol_file.write("," + str(stock_symbol_vol) + "," + str(stock_symbol_vol_apds))
                stock_symbol_file.write("," + str(vol_accuracy) + "\n")

            symbol_count = len(stock_vol_dist)
            avg_freq_accuracy = total_freq_accuracy / symbol_count

            symbol_count = len(stock_vol_dist)
            avg_vol_accuracy = total_vol_accuracy / symbol_count
            #apds_size = str(os.path.getsize(apds_filename)/1024)
            apds_size = 0

            #stock_vol_apds.export(apds_filename)
            stock_vol_apds.clear()

            stock_symbol_file.write("Averge," + str(stock_trade_record_count))
            stock_symbol_file.write("," + str(width) + "," + str(depth))
            stock_symbol_file.write(",,," + str(avg_freq_accuracy) + ",,," + str(avg_vol_accuracy) + "\n")

            timenow = datetime.datetime.now()
            apds_endtime = str(timenow.strftime("%x")) + ' ' + str((timenow.strftime("%X")))

            process_file.write(str(stock_trade_record_count) + "," + str(symbol_count) + "," + str(width) + "," + str(depth))
            process_file.write("," + str(avg_freq_accuracy) + "," + str(avg_vol_accuracy) + "," + str(apds_size))
            process_file.write("," + str(stock_vol_apds.confidence) + "," + str(stock_vol_apds.error_rate))
            process_file.write("," + str(vol_sketch_time) + "," + str(vol_sketch_qrytime) + "," + apds_starttime + "," + apds_endtime + "\n")

            #stock_input_file.close()
            stock_trade_file.close()
            stock_symbol_file.close()

        process_file.write("\n")

process_file.close()

process_endtime = time.process_time()
prg_endtime = time.asctime(time.localtime(time.time()))
print('Program Start Time: ' + prg_starttime + " Run Time : " + str(process_endtime - process_starttime) + " End Time: " + prg_endtime)

# end of the program