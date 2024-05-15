import matplotlib.pyplot as plt
import matplotlib
import numpy as np
import sys
import time
import datetime
import pandas
import timeit
import os
import scipy
import csv
import io

import getmaindata	#Fluid DAQ data from the Arduino ADC 
import getesdata	#Current data logged by the eslogger.py script and the ADAM ADC module on the main skid
import getpsudata	#Total current and voltage data logged by Joe's LabVIEW panel





startdatestr = '2024-05-13T12-10-00'
enddatestr = '2024-05-13T13-30-00'
datarate = 1	#Hz. If source data rate is less than this, it will be interpolated.







if __name__ == '__main__':
	startdate = datetime.datetime.strptime(startdatestr, '%Y-%m-%dT%H-%M-%S')
	enddate = datetime.datetime.strptime(enddatestr, '%Y-%m-%dT%H-%M-%S')


	maindf = getmaindata.getmaindata().set_index('Time')	#Use the 'Time' column (which contains datetime objects) as the index.
															#We will insert another column later which also has the name 'Time'.
															#Don't get confused.
	esdf = getesdata.getesdata().set_index('Time')
	psudf = getpsudata.getpsudata().set_index('Time')

	date = startdate

	finaldf = maindf.join(esdf)
	finaldf = finaldf.join(psudf)

	#Apply start and end dates
	finaldf = finaldf[startdate:enddate]

	finaldf = finaldf.resample(datetime.timedelta(microseconds = int((1e6)*(1/datarate)))).interpolate()

	finaldf['ISOTime'] = finaldf.index.map(lambda x: str(x.isoformat()))	#Create the ISO timestamp column
	temp = finaldf.pop('ISOTime')	#Remove it...

	finaldf.insert(0, 'Time', temp)	#...and re-insert it at the left as 'Time'.

	#Compute the power columns. 
	finaldf['P1'] = finaldf['I1']*finaldf['Voltage (V)']
	finaldf['P2'] = finaldf['I2']*finaldf['Voltage (V)']
	finaldf['P3'] = finaldf['I3']*finaldf['Voltage (V)']
	finaldf['P4'] = finaldf['I4']*finaldf['Voltage (V)']
	finaldf['P5'] = finaldf['I5']*finaldf['Voltage (V)']
	finaldf['P6'] = finaldf['I6']*finaldf['Voltage (V)']

	print(finaldf)

	outputfilename = f'{startdatestr}_to_{enddatestr}_consolidated.csv'	#Processed filename has the start date as name
	finaldf.to_csv(outputfilename, index=False)
	print(f'Wrote processed CSV to file.')