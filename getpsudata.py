import datetime
import os
import io
import numpy 
import pandas
import csv

datadir = './psudata'

def getpsudata():
	print(f'======== Loading PSU power data')

	if os.path.exists(datadir) and os.path.isdir(datadir):
		fileanddirlist = os.listdir(datadir)

		filelist = []
		for item in fileanddirlist:	#Separate the contents of the data directory. Disregard directories, keep files in list
			if not os.path.isdir(os.path.join(datadir, item)):
				filelist.append(item)
	else:
		print("Data directory does not exist.")
		return None
		
		
	#filequerylist = []
	#querydate = startdate

	#while(querydate < enddate):
	#	filequerylist.append(querydate.strftime("%Y-%m-%dT%H.csv"))
	#	querydate = querydate + datetime.timedelta(hours=1)

	#print(f'{len(filequerylist)} files of interest:')
	#print(filequerylist)

	print(f'{len(filelist)} files available:')
	#print(filelist)

	data = []	#Array to hold all the data

	for file in filelist:
		path = os.path.join(datadir, file)
		print(f'Loading: {path}')
		contents = open(path, 'r').read()

		f = io.StringIO(contents)
		reader = csv.reader(f, delimiter='\t')
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

					datacolumns = len(header)
					print(f'Header: {header}')
					print(f'{datacolumns} columns.')
					
				rowindex+=1
				continue	#Ignore rows before first header line
			
				
				
			if row[0] == 'Time':	#This row is a header, not data. Skip it
				rowindex+=1
				continue
			
			date = None
			try:	#Re-construct the date with 1.) the year, month, and day from the filename and 2.) the hour, minute, and seconds from the timestamp.
				timestampfields = row[0].split(':')
				year = int(f'20{file[0:2]}')
				month = int(f'{file[2:4]}')
				day = int(f'{file[4:6]}')
				hour = int(timestampfields[0])
				minutes = int(timestampfields[1])
				seconds = int(float(timestampfields[2]))
				microseconds = int((1e6)*(float(timestampfields[2])%1))
				date = datetime.datetime(year, month, day, hour, minutes, seconds, microseconds)
				#print(date)
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

			if rowindex > 5:	#####
				break
			
			
	print(f'Loaded {len(data)} rows')
		
	rowindex+=1
	#return header, data
	
	#Build dataframe
	
	df = pandas.DataFrame(data)

	#Set header
	df.columns = header

	#print('Sorting...')
		 
	df.set_index('Time')
	df.sort_values(by=['Time'])

	#print(df)

	return df