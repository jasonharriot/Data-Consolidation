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





startdatestr = '2024-05-21T12-50-00'
enddatestr = '2024-05-21T12-52-00'
datarate = 1	#Hz.







if __name__ == '__main__':
	startdate = datetime.datetime.strptime(startdatestr, '%Y-%m-%dT%H-%M-%S')
	enddate = datetime.datetime.strptime(enddatestr, '%Y-%m-%dT%H-%M-%S')


	maindf = getmaindata.getmaindata().set_index('Time')[startdate:enddate]	#Use the 'Time' column (which contains datetime objects) as the index.
															#We will insert another column later which also has the name 'Time'.
															#Don't get confused.

	#print(f'main dataframe:')
	#print(maindf[0:50])

	esdf = getesdata.getesdata().set_index('Time')[startdate:enddate]
	#print(f'ES dataframe:')
	#print(esdf[0:50])

	psudf = getpsudata.getpsudata().set_index('Time')[startdate:enddate]



	#print(f'======== Combining dataframes')
	#print(f'main + ES dataframe + PSU dataframe:')
	finaldf = maindf.join(esdf, sort=True, how='outer').join(psudf, how='outer')

	print(f'======== Forward-filling dataframes')
	finaldf = finaldf.ffill(limit=100)
	#print(f'forward-filled:')
	#print(finaldf[0:50])


	#print(f'Is unique before... Index: {finaldf.index.is_unique} Columns: {finaldf.columns.is_unique}')
	finaldf = finaldf.groupby(finaldf.index).first()	#This removes rows with duplicate index. It should not remove very many rows.
	#print(f'Is unique after... Index: {finaldf.index.is_unique} Columns: {finaldf.columns.is_unique}')
	#print(f'De-duplicated dataframe (may have no change):')
	#print(finaldf[0:50])


	resampletimedelta=datetime.timedelta(microseconds = int((1e6)*(1/datarate)))
	#print(f'The time delta for resampling is: {resampletimedelta.seconds} s')
	#finaldf = finaldf.resample(resampletimedelta).interpolate(method='time



	finaldf = finaldf.resample(resampletimedelta).interpolate()




	#print(f'Re-sampled dataframe:')
	#print(finaldf[0:50])

	isotime = finaldf.index.map(lambda x: str(x.isoformat()))	#Create the ISO timestamp column
	#temp = finaldf.pop('ISOTime')	#Remove it...

	finaldf.insert(0, 'Time', isotime)	#insert it at the left as 'Time'.

	#Compute the power columns. 
	finaldf['P1'] = finaldf['I1']*finaldf['Voltage (V)']
	finaldf['P2'] = finaldf['I2']*finaldf['Voltage (V)']
	finaldf['P3'] = finaldf['I3']*finaldf['Voltage (V)']
	finaldf['P4'] = finaldf['I4']*finaldf['Voltage (V)']
	finaldf['P5'] = finaldf['I5']*finaldf['Voltage (V)']
	finaldf['P6'] = finaldf['I6']*finaldf['Voltage (V)']

	
	print(f'Final dataframe: ')
	print(finaldf)



	#Print a lot of rows from the dataframe so we can see them all. For debugging.
	#pandas.set_option('display.max_rows', 500)
	#print(finaldf[0:300])




	outputfilename = f'{startdatestr}_to_{enddatestr}_consolidated.csv'	#Processed filename has the start date as name
	finaldf.to_csv(outputfilename, index=False)
	print(f'Wrote processed CSV to file.')