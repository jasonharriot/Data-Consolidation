import datetime
import os
import io
import numpy 
import pandas
import sys
import csv

datadir = './esdata'

def getesdata():
	print(f'======== Loading ES cell current data')
	if os.path.exists(datadir) and os.path.isdir(datadir):
		fileanddirlist = os.listdir(datadir)

		filelist = []
		for item in fileanddirlist:	#Separate the contents of the data directory. Disregard directories, keep files in list
			if not os.path.isdir(os.path.join(datadir, item)):
				filelist.append(item)
	else:
		print("Data directory does not exist.")
		return None
		
		

	datastr = ''	#Holds all data direct from file

	#filequerylist = []
	#querydate = startdate

	#while(querydate < enddate):
	#	filequerylist.append(querydate.strftime("%Y-%m-%dT%H.csv"))
	#	querydate = querydate + datetime.timedelta(hours=1)

	#print(f'{len(filequerylist)} files of interest:')
	#print(filequerylist)

	print(f'{len(filelist)} files available:')
	#print(filelist)

	for file in filelist:
		path = os.path.join(datadir, file)
		print(f'Loading: {path}')
		datastr += open(path, 'r').read()


	data = []	#Array to hold all the data, in proper order. A dataframe will be made from this later on, but constructing the data as a simple array first is fastest.


	#print('Preparing fields')
	
	
	f = io.StringIO(datastr)
	reader = csv.reader(f, delimiter=',')
	headerfound = False
	datacolumns = 0	#Number of columns found in the header row. Check against data rows.

	header = None	#Header row for pandas dataframe
	rowindex = 0
	
	for row in reader:
		#print(f'Parsing: {row}')
		
		if len(row) < 3:
			rowindex+=1
			continue
			
		if not headerfound:
			if row[0] == 'Time':	#The "Time" marker appears in the first column of the header. Use it as an indicator.
			
				print(f'Header found at {rowindex}')
				
				#Header row
				headerfound = True
				header = row

				for i in range(0, len(header)):
					sys.stdout.write(f'Changing header val {header[i]} to ')
					header[i] = header[i].replace('V', 'I')
					print(f'{header[i]}')

				datacolumns = len(header)
				print(f'Header: {header}')
				print(f'{datacolumns} columns.')
				
			rowindex+=1
			continue	#Ignore rows before first header line
		
			
			
		if row[0] == 'Time':	#This row is a header, not data. Skip it
			rowindex+=1
			continue
		
		date = None
		try:
			datestr = row[0]

			if len(datestr) == 22:
				hundreths = datestr[20:]
			else:
				#print(f'Length of datstamp is {len(datestr)}! Defaulting timestamp hundreths')
				hundreths = 0

			microseconds = int(hundreths)*10000	#Convert hundreths to microseconds because that's the only field that we can parse with datetime
			datestr = datestr[:19]
			datestr += f'_{microseconds:06d}'
			date = datetime.datetime.strptime(datestr, "%Y-%m-%dT%H-%M-%S_%f")
		except Exception as e:
			print("Couldn't parse timestamp:")
			print(row[0])
			print(e)
			rowindex+=1
			continue
			
		#if date < startdate:
		#	rowindex+=1
		#	continue
			
		#if not enddate is None and date > enddate:
		#	break
			
		#print(date)
		
		datarow = [date]
		
		for i in range(1, len(row)):	#Skip the original datestamp, we have added the date object to the data row. We will not use the datstamp.
			if len(row[i]) < 1:
				rowindex+=1
				continue
				
			if row[i] == '' or row[i] is None:
				rowindex+=1
				continue
				
				
			try:
				value = float(row[i])
			except:
				value = 0
				print(f'Warning: defaulting value to zero: row {rowindex}, {i}')
				
			#print(f'Including elemnt {i} ({row[i]})')
				
			datarow.append(value)
			
		if not len(datarow) == datacolumns:
			print(f'Data row invalid:')
			print(datarow)
			pass
		
		else:
			data.append(datarow)
			
			
	print(f'Loaded {len(data)} rows')
		
	rowindex+=1
	#return header, data
	
	#Build dataframe
	
	df = pandas.DataFrame(data)

	#Set header
	df.columns = header

	#print('Sorting...')

	df = df.set_index('Time').sort_index()
	

	#Apply sensor calibrations
	df['I1'] = df['I1']*-1489.97
	df['I2'] = df['I2']*-1489.97
	df['I3'] = df['I3']*-1489.97
	df['I4'] = df['I4']*-1489.97
	df['I5'] = df['I5']*-1489.97
	df['I6'] = df['I6']*-1489.97

	#print(df)

	return df