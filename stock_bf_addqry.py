# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Author Ramesh, Created Aug 29, 2019
# Phase-II BF Workings
# Program to add stock symbol to bitarray and analysis
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

import time;
import datetime;
import csv
import os
from probables import (BloomFilter)

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

no_of_records = [800]
#no_of_records = [1000]
#no_of_records = [1000,2000,3000]
#no_of_records = [2000000,4000000,6000000,8000000,10000000]
#no_of_records = [10000,20000,30000,40000,50000,60000,70000,80000,90000,100000]
#no_of_records = [6000000,7000000,8000000]
#no_of_records = [1000000,2000000,3000000,4000000,5000000,6000000,7000000,8000000,9000000,10000000,11000000,12000000,13000000,14000000,15000000]

est_elements = [1000]
#est_elements = [1000,2000,3000]
#est_elements = [1000,2000,3000,4000,5000,6000,7000,8000,9000,10000]

false_positive_rates = [0.05]
#false_positive_rates = [0.05, 0.025, 0.0125]
#false_positive_rates = [0.05, 0.025, 0.0125, 0.005, 0.0025, 0.00125]

phd_dir = "C:/My Cloud/GoogleDrive/1 PhD/"
phd_data_dir = phd_dir + "data/"
proj_dir = phd_dir + "apds/stock/"
proj_data_dir = phd_data_dir + "stock/"
#proj_temp_dir = phd_dir + "apds/temp/"

process_filename = proj_dir + stock_etf + time_interval  + "_bf_process_" + str(dateval) + ".csv"
process_file = open(process_filename, "w")

for no_of_record in no_of_records:
    process_file.write('Est. Elements Added(BF)')
    process_file.write(',Est. Elements Planned')
    process_file.write(',Stocks Added')
    process_file.write(',Estimated Unique Elements')
    process_file.write(',False +ve Count')
    process_file.write(',False +ve Percent')
    process_file.write(',Bit Array Size')
    process_file.write(',Hash Functions')

    process_file.write(',No of Input Records')
    process_file.write(',False +ve Rate Planned')
    process_file.write(',False +ve Rate(Actual)')

    process_file.write(',Memory Size(Bits)')
    process_file.write(',Export File Size(Bytes)')
    process_file.write(',Check Stock Count')
    process_file.write(',Probably Traded Count')
    process_file.write(',Traded Count')

    process_file.write(',Definitely Not Traded Count')
    process_file.write(',BF Create Time')
    process_file.write(',Prg Start Time')
    process_file.write(',Prg End Time')
    process_file.write(',Input File Name')
    process_file.write(',Bloom Filter Name\n')

    # process_file.write('No of Input Records, Est. Elements Planned, Est. Elements Added(BF)')
    # process_file.write(',Estimated Unique Elements, False +ve Rate Planned, False +ve Rate(Actual), Bit Array Size')
    # process_file.write(',Hash Functions, Memory Size(Bits), Export File Size(Bytes)')
    # process_file.write(',Stocks Added, Check Stock Count, Probably Traded Count, Traded Count')
    # process_file.write(',False +ve Traded Count, Definitely Not Traded Count')
    # process_file.write(',BF Create Time, Prg Start Time, Prg End Time')
    # process_file.write(',Input File Name, Bloom Filter Name\n')

    for est_element in est_elements:
        for false_positive_rate in false_positive_rates:
            timenow = datetime.datetime.now()
            bf_starttime =  str(timenow.strftime("%x"))+ ' ' + str((timenow.strftime("%X")))
            #stock_trade_filename = proj_data_dir + stock_etf + "_trade_" + time_interval + "_R" + str(no_of_record) + ".csv"
            stock_trade_filename = proj_data_dir + "preprocessed/" + stock_etf + "_trade_" + time_interval + "_R15000000.csv"

            stock_trade_file = open(stock_trade_filename,"r")
            stock_trade_lines = csv.reader(stock_trade_file, delimiter=',', quoting=csv.QUOTE_ALL, skipinitialspace=True)
            next(stock_trade_lines)
            print('No of Input Rec:', str(no_of_record),' Estimated Element:', str(est_element),' False Positive Rate:', str(false_positive_rate), ' BF Start Time:', bf_starttime)

            stock_symbol_filename = proj_dir + "stock_exists_files/" + stock_etf + time_interval + "_R" + str(no_of_record) + "_w" + str(est_element) + "_d" + str(false_positive_rate) + "_exists.csv"
            stock_symbol_file = open(stock_symbol_filename, "w")
            stock_symbol_file.write('Stock Symbol, Traded or Not\n')

            stock_exists_bf_filename = proj_dir + "bf_files/" + stock_etf + time_interval + "_R" + str(no_of_record) + "_w" + str(est_element) + "_d" + str(false_positive_rate) + "_exists.bf"

            stock_exists_bf = BloomFilter(est_elements=est_element, false_positive_rate=false_positive_rate)
            stock_symbol_dist = {}
            stock_trade_record_count = 0
            bfadd_time = 0

            #Add memebers to Bit Array
            for stock_trade_line in stock_trade_lines:
                stock_trade_record_count = stock_trade_record_count + 1
                if stock_trade_record_count > no_of_record: break
                stock_symbol = stock_trade_line[0].strip()

                bfadd_starttime = time.process_time()
                memadd = stock_exists_bf.add(stock_symbol)
                bfadd_endtime = time.process_time()
                bfadd_time = bfadd_time + (bfadd_endtime - bfadd_starttime)

                if stock_symbol not in stock_symbol_dist.keys():
                    #print('Added Unique Stock:',stock_symbol)
                    stock_symbol_dist.update({stock_symbol: 1})

            #Read Check Stock Symbol File
            check_stock_trade_filename = phd_data_dir + "stock/stock_traded_memcheck.csv"
            check_stock_trade_file = open(check_stock_trade_filename,"r")
            check_stock_trade_lines = csv.reader(check_stock_trade_file, delimiter=',', quoting=csv.QUOTE_ALL, skipinitialspace=True)
            next(check_stock_trade_lines)

            check_stock_trade_record_count = 0
            check_stock_symbol_count = 0
            prob_present_count = 0
            present_count = 0
            false_postive_count = 0
            not_present_count = 0

            #Query Check Stock Symbol in Created BitArray
            for check_stock_trade_line in check_stock_trade_lines:
                check_stock_trade_record_count = check_stock_trade_record_count + 1
                check_stock_symbol = check_stock_trade_line[0].strip()
                check_stock_symbol_count = check_stock_symbol_count + 1

                if check_stock_symbol in stock_exists_bf:
                    prob_present_count = prob_present_count + 1
                    if check_stock_symbol in stock_symbol_dist.keys():
                        present_count = present_count + 1
                        stock_symbol_file.write(check_stock_symbol + "," + ' Traded\n')
                    else:
                        false_postive_count = false_postive_count + 1
                        stock_symbol_file.write(check_stock_symbol + "," + ' Traded - False Positive\n')
                else:
                    not_present_count = not_present_count + 1
                    stock_symbol_file.write(check_stock_symbol + "," + ' Defenitely Not Traded\n')
            stock_symbol_file.write('Probably Traded Count,' + str(prob_present_count) + "\n")
            stock_symbol_file.write('Traded Count,' + str(present_count) + "\n")
            stock_symbol_file.write('False +ve Traded Count,' + str(false_postive_count) + "\n")
            stock_symbol_file.write('Definitely Not Traded Count,' + str(not_present_count) + "\n")

            stock_trade_file.close()
            stock_exists_bf.export(stock_exists_bf_filename)

            timenow = datetime.datetime.now()
            bf_endtime = str(timenow.strftime("%x")) + ' ' + str((timenow.strftime("%X")))

            est_elemnts_added_bf = stock_exists_bf.elements_added
            est_elements_planned = stock_exists_bf.estimated_elements
            stocks_added = len(stock_symbol_dist)
            est_unique_elements = stock_exists_bf.estimate_elements()

            process_file.write(str(est_elemnts_added_bf))
            process_file.write("," + str(est_elements_planned))
            process_file.write("," + str(stocks_added))
            process_file.write("," + str(est_unique_elements))
            process_file.write("," + str(false_postive_count))
            process_file.write("," + str(false_postive_count/est_unique_elements))
            process_file.write("," + str(stock_exists_bf.bloom_length))
            process_file.write("," + str(stock_exists_bf.number_hashes))

            process_file.write("," + str(no_of_record))
            process_file.write("," + str(stock_exists_bf.false_positive_rate))
            process_file.write("," + str(stock_exists_bf.current_false_positive_rate()))

            process_file.write("," + str(stock_exists_bf.number_bits))
            process_file.write("," + str(stock_exists_bf.export_size()))

            process_file.write("," + str(check_stock_symbol_count))
            process_file.write("," + str(prob_present_count))
            process_file.write("," + str(present_count))

            process_file.write("," + str(not_present_count))
            process_file.write("," + str(bfadd_time))
            process_file.write("," + bf_starttime + "," + bf_endtime)
            process_file.write("," + stock_trade_filename + ',' + stock_exists_bf_filename + "\n")
    process_file.write("\n")
process_file.close()

process_endtime = time.process_time()
prg_endtime = time.asctime(time.localtime(time.time()))
print('Program Start Time: ' + prg_starttime + " Run Time : " + str(process_endtime - process_starttime) + " End Time: " + prg_endtime)

# end of the program
