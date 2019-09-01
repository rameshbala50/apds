'''
This program developed to add elements the sketch (CMS)
Developed by: Ramesh FEB-28-2019
'''

import calendar;
import time;
import datetime;
import csv
from countminsketch import CountMinSketch

today = datetime.datetime.now()
dateval = today.strftime("%y") + today.strftime("%d") + today.strftime("%m") + today.strftime("%H") + today.strftime("%M") + today.strftime("%S")
prg_starttime = time.process_time()
print ('Program Start Time: ' + str(today.strftime("%x")) + ' ' + str((today.strftime("%X"))))

#time_interval = "5min"
#time_interval = "hourly"
time_interval = "daily"

stock_etf = "stock"
#stock_etf = "etf"

width = 10000
depth = 6

#confidence = 0.984375
#confidence = 0.999
#error_rate = 0.0004

#no_of_records = [1000000,2000000,3000000,4000000,5000000,6000000,7000000,8000000,9000000,10000000,11000000,12000000,13000000,14000000,15000000]
no_of_records = [100000,200000,300000,400000,500000,600000,700000,800000,900000,1000000]
#no_of_records = [100000,200000,300000,400000,500000]
no_of_records = [10000]

data_files = []

source_data_dir = "C:/My Cloud/GoogleDrive/1 PhD/data/stock/"
project_dir = "C:/My Cloud/GoogleDrive/1 PhD/cms/"
data_dir = project_dir + "data/"
output_dir = project_dir + "stock/"
check_stock_symbol_filename = data_dir + stock_etf + "_" + time_interval + "_symbols_check.csv"

print (check_stock_symbol_filename)
processinfo_filename = output_dir + stock_etf + "_" + time_interval + "_cms_analysis_" + str(dateval) + ".csv"
processinfo_file = open(processinfo_filename, "w")
processinfo_file.write('Sketch Width, No of hash functions')
processinfo_file.write(',No of Trade Records, No of Stock Symbols')
processinfo_file.write(',CrossRef Not Present Count, CrossRef Present Count')
processinfo_file.write(',Not Present Count, Present Count,Error Count,Error %')
processinfo_file.write(',CMS Time, Sketch Time, Sketch Save Time, Sketch Query Time, CMS Query Time')
processinfo_file.write(',CMS Start Time\n')

for no_of_record in no_of_records:
    stock_trade_filename = source_data_dir + stock_etf + "_trade_" + time_interval + str(no_of_record) + '.csv'
    crossref_not_present_count = 0
    crossref_present_count = 0

    #print('CrossRef CMS Process Starts Time: ' + str(today.strftime("%X")) + ' ' + str(today.strftime("%f")))
    #CrossRef CMS Process Starts
    crossref_stock_trade_frq_cms = CountMinSketch(100000, 10)
    stock_trade_file = open(stock_trade_filename,"r")
    stock_trade_lines = csv.reader(stock_trade_file, delimiter=',', quoting=csv.QUOTE_ALL, skipinitialspace=True)
    next(stock_trade_file)

    #print('CrossRef CMS Create Time: ' + str(today.strftime("%X")) + ' ' + str(today.strftime("%f")))
    #CrossRef CMS Create
    for stock_trade_line in stock_trade_lines:
        stock_symbol = stock_trade_line[0].strip()
        add1 = crossref_stock_trade_frq_cms.add(stock_symbol)
    stock_trade_file.close()

    #print('CrossRef CMS Membership Check Time: ' + str(today.strftime("%X")) + ' ' + str(today.strftime("%f")))
    #CrossRef CMS Membership Check
    check_stock_symbol_file = open(check_stock_symbol_filename, "r")

    for stock_symbol_line in check_stock_symbol_file:
        stock_symbol = stock_symbol_line.strip()
        stock_frq = crossref_stock_trade_frq_cms.check(stock_symbol)
        if stock_frq == 0: crossref_not_present_count = crossref_not_present_count + 1
        else: crossref_present_count = crossref_present_count + 1
    check_stock_symbol_file.close()
    #CrossRef CMS Process Completed
    #print('CrossRef CMS Process Completed Time: ' + str(today.strftime("%X")) + ' ' + str(today.strftime("%f")))

    #Actual CMS Process Starts
    stock_trade_file = open(stock_trade_filename,"r")
    stock_trade_lines = csv.reader(stock_trade_file, delimiter=',', quoting=csv.QUOTE_ALL, skipinitialspace=True)
    next(stock_trade_file)

    stock_trade_record_count = 0

    cms_starttime = time.process_time()
    cms_starttime2 = time.asctime(time.localtime(time.time()))
    stock_trade_frq_cms = CountMinSketch(width=width, depth=depth)
    #stock_trade_frq2_cms = CountMinSketch(width=width, depth=depth)

    sketch_time = 0

    #print('CMS Create Time: ' + str(today.strftime("%X")) + ' ' + str(today.strftime("%f")))
    #CMS Create
    for stock_trade_line in stock_trade_lines:
        stock_trade_record_count = stock_trade_record_count + 1
        stock_symbol = stock_trade_line[0].strip()

        sketch_starttime = time.process_time()
        add1 = stock_trade_frq_cms.add(stock_symbol)
        sketch_endtime = time.process_time()
        sketch_time = sketch_time + (sketch_endtime - sketch_starttime)

    cms_sketch_name = output_dir + stock_etf + "_trade_" + time_interval + str(no_of_record) + '.cms'

    sketch_endtime2 = time.process_time()
    stock_trade_frq_cms.export(cms_sketch_name)
    cms_endtime = time.process_time()
    cms_endtime2 = time.asctime(time.localtime(time.time()))

    sketch_save_time = cms_endtime - sketch_endtime2
    stock_trade_file.close()

    stock_trade_frq_cms = CountMinSketch(filepath=cms_sketch_name)

    #print('CMS Membership Check Time: ' + str(today.strftime("%X")) + ' ' + str(today.strftime("%f")))
    #CMS Membership Check
    check_stock_symbol_file = open(check_stock_symbol_filename, "r")
    check_stock_symbol_count = 0
    not_present_count = 0
    present_count = 0
    sketch_query_time = 0
    cms_query_starttime = time.process_time()
    cms_query_starttime2 = time.asctime(time.localtime(time.time()))

    for stock_symbol_line in check_stock_symbol_file:
        check_stock_symbol_count = check_stock_symbol_count + 1
        stock_symbol = stock_symbol_line.strip()

        sketch_query_starttime = time.process_time()
        stock_frq = stock_trade_frq_cms.check(stock_symbol)
        sketch_query_endtime = time.process_time()
        sketch_query_time = sketch_query_time + (sketch_query_endtime - sketch_query_starttime)

        if stock_frq == 0: not_present_count = not_present_count + 1
        else: present_count = present_count + 1
    check_stock_symbol_file.close()

    cms_query_endtime = time.process_time()
    cms_query_endtime2 = time.asctime(time.localtime(time.time()))

    print ("Analyzed - Stock Trade Records: " + str(stock_trade_record_count) + " Stock Symbol: " + str(check_stock_symbol_count) + " Run Time : " + str(cms_query_endtime - cms_starttime))

    processinfo_file.write(str(width) + "," + str(depth))
    processinfo_file.write("," + str(stock_trade_record_count) + "," + str(check_stock_symbol_count))
    processinfo_file.write("," + str(crossref_not_present_count) + "," + str(crossref_present_count))
    processinfo_file.write("," + str(not_present_count) + "," + str(present_count) + "," + str(not_present_count - crossref_not_present_count))
    processinfo_file.write("," + str((not_present_count - crossref_not_present_count)/stock_trade_record_count))
    processinfo_file.write("," + str(cms_endtime - cms_starttime) + "," + str(sketch_time) + "," + str(sketch_save_time))
    processinfo_file.write("," + str(sketch_query_time) + "," + str(cms_query_endtime - cms_query_starttime))
    processinfo_file.write("," + str(cms_starttime2))
    processinfo_file.write("\n")

processinfo_file.close()

prg_endtime = time.process_time()
prg_endtime2 = time.asctime(time.localtime(time.time()))
print ('Program Start Time: ' + str(today.strftime("%x")) + ' ' + str((today.strftime("%X"))) + " Run Time : " + str(prg_endtime - prg_starttime))

# end of the program
